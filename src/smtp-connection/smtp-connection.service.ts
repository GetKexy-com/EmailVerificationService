import { Injectable } from '@nestjs/common';
import * as net from 'net';
import * as tls from 'tls';
import {
  EmailReason,
  EmailStatus,
  EmailStatusType,
  EmailValidationResponseType,
  ipBlockedStringsArray,
  SMTPResponseCode,
} from '@/common/utility/email-status-type';
import { WinstonLoggerService } from '@/logger/winston-logger.service';
import { randomStringGenerator } from '@nestjs/common/utils/random-string-generator.util';
import { SMTP_RESPONSE_MAX_DELAY } from '@/common/utility/constant';
import freeEmailProviderList from '@/common/utility/free-email-provider-list';
import { RetryStatus } from '@/domains/entities/processed_email.entity';
import { BulkFileEmail } from '@/bulk-file-emails/entities/bulk-file-email.entity';

@Injectable()
export class SmtpConnectionService {
  private socket: net.Socket | tls.TLSSocket;
  private host: string;
  private readonly socketTimeout: number = 10000;
  private readonly socketEncoding: any = 'utf-8';
  private readonly port: number = 25;
  private readonly tlsPort: number = 587;
  private readonly tlsMinVersion: any = 'TLSv1.2';
  private isSmtpSlow: boolean = false;

  constructor(
    private winstonLoggerService: WinstonLoggerService,
  ) {
  }

  async connect(mxHost: string): Promise<any> {
    this.host = mxHost;
    return new Promise(async (resolve, reject): Promise<any> => {
      let socketDataFired = false;
      this.socket = net.createConnection({ host: this.host, port: this.port, family: 4 }, () => {
        console.log('Socket connected!');
      });
      this.socket.setEncoding(this.socketEncoding);
      this.socket.setTimeout(this.socketTimeout);

      this.socket.once('data', async (data) => {
        socketDataFired = true;
        try {
          const ehloResponse = await this.sendCommand(`EHLO ${this.host}`);
          const ehloStatus = this.parseSmtpResponseData(ehloResponse, mxHost);
          if (ehloStatus.status !== EmailStatus.VALID) {
            reject(ehloStatus);
            return;
          }
          if (!ehloResponse.includes('STARTTLS')) {
            // If ehloResponse not rejecting and does not have "STARTTLS" then we have to resolve()
            // here to continue using the unencrypted socket.
            console.error('❌ STARTTLS Not Found. Using regular socket');
            resolve(true);
            return;
          }

          await this.sendCommand(`STARTTLS`);

          // Step 3: Upgrade Connection to TLS
          console.log('🔒 Upgrading to TLS...');
          const secureSocket = tls.connect(
            {
              socket: this.socket,
              port: this.tlsPort,
              host: this.host,
              timeout: this.socketTimeout,
              minVersion: this.tlsMinVersion, // Specify minimum TLS version
              servername: this.host,
              rejectUnauthorized: false, // Allow self-signed certificates
            },
            () => {
              if (secureSocket.encrypted) {
                console.log('✅ TLS secured. Ready to authenticate.');
                this.socket.removeAllListeners();
                // Replace with secure socket
                this.socket = secureSocket;
                resolve(true);
              }
            },
          );
          // This socket error handles if any issue when connecting to TLS
          secureSocket.once('error', (err) => {
            console.error('❌ TLS Upgrade Error:', err);
            const error: EmailStatusType = {
              status: EmailStatus.INVALID,
              reason: EmailReason.DOES_NOT_ACCEPT_MAIL,
            };
            reject(error);
            // If STARTTLS is available but does not let us upgrade, we QUIT from it.
            this.sendCommand(`QUIT`);
            return;
          });
          secureSocket.once('close', () => {
            console.log('secureSocket closed');
            const error: EmailStatusType = {
              status: EmailStatus.INVALID,
              reason: EmailReason.MAILBOX_NOT_FOUND,
            };
            reject(error);
            this.sendCommand(`QUIT`);
            return;
          });
          secureSocket.once('timeout', () => {
            console.log('secureSocket timeout');
            const error: EmailStatusType = {
              status: EmailStatus.UNKNOWN,
              reason: EmailReason.SMTP_TIMEOUT,
            };
            resolve(error);
            this.sendCommand(`QUIT`);
            return;
          });

        } catch (e) {
          // If "EHLO" command throw exception then it is caught here
          const emailStatus: EmailStatusType = { status: undefined, reason: undefined };
          if (e === EmailReason.IP_BLOCKED) {
            emailStatus.status = EmailStatus.SERVICE_UNAVAILABLE;
            emailStatus.reason = EmailReason.IP_BLOCKED;
          } else if (e === EmailReason.DOES_NOT_ACCEPT_MAIL) {
            emailStatus.status = EmailStatus.INVALID;
            emailStatus.reason = EmailReason.DOES_NOT_ACCEPT_MAIL;
          } else if (e === EmailReason.GREY_LISTED) {
            emailStatus.status = EmailStatus.UNKNOWN;
            emailStatus.reason = EmailReason.GREY_LISTED;
          } else {
            emailStatus.status = EmailStatus.INVALID;
            emailStatus.reason = e;
          }
          reject(emailStatus);
          return;
        }
      });
      // This socket error handles if any issue when connecting to email server
      // This usually means the mailbox is invalid
      this.socket.once('error', (err) => {
        console.error('❌ SMTP Connection Error:', err);
        const error: EmailStatusType = {
          status: EmailStatus.INVALID,
          reason: EmailReason.DOES_NOT_ACCEPT_MAIL,
        };
        reject(error);
        return;
      });


      this.socket.once('close', () => {
        console.log('closed');
        if (!socketDataFired) {
          const error: EmailStatusType = {
            status: EmailStatus.INVALID,
            reason: EmailReason.MAILBOX_NOT_FOUND,
          };
          reject(error);
          return;
        }
      });
      this.socket.once('timeout', () => {
        console.log('timeout');
        const error: EmailStatusType = {
          status: EmailStatus.UNKNOWN,
          reason: EmailReason.SMTP_TIMEOUT,
        };
        resolve(error);
        return;
      });
    });
  }

  private async sendCommand(command: string, email = ''): Promise<string> {
    // Before sending a new command, try to remove previous command listeners to
    // avoid getting old stream response in socket.on('data')
    // this.socket.removeAllListeners();
    let timeout: NodeJS.Timeout;
    // Track command start time to detect calculate server response time.
    let startTime = Date.now();

    return new Promise((resolve, reject) => {
      try {
        // Because of BottleNeck concurrency, sometime call stack calls a socket which
        // was already closed previously. Doing this causes an error -
        // Unhandled Promise Rejection: Error: write EPROTO
        // To avoid such error. we check if the socket exist or not.
        // If it does then we add these to 'GREY_LISTED' queue to re-check
        if (!this.socket || this.socket.destroyed || this.socket.closed) {
          reject(EmailReason.GREY_LISTED);
          return;
        }
        this.socket.write(command + '\r\n', 'utf-8', (err: Error) => {
          if (err) {
            console.error('Socket write error:', err);
            reject(err);
          }
          console.debug(`➡ Sent: ${command}`);
        });

        let responseData = '';

        this.socket.on('data', (chunk) => {
          const responseTime = Date.now() - startTime;
          responseData = chunk.toString();
          console.debug(`⬅ Received: ${responseData}`);

          // Detect if mail server is slow or not.
          if (command.includes('EHLO') && responseTime > SMTP_RESPONSE_MAX_DELAY) {
            console.log({ responseTime });
            this.isSmtpSlow = true;
          }

          // If it's a slow server then we have to wait 2 sec to get the full 'stream'
          // response from socket.on('data'). Otherwise we can't find out the last response
          // to know the status of the email.
          if (this.isSmtpSlow) {
            clearTimeout(timeout); // Reset timeout on new data
            timeout = setTimeout(() => {
              resolve(responseData);
            }, 2000);
          } else {
            resolve(responseData);
            return;
          }
        });

        this.socket.once('close', () => {
          // If the socket is closed by the SMTP server without letting us complete
          // all commands then it probably blocked our IP. But if all commands
          // completed and SMTP response has code above 400, the email address is invalid
          if (responseData) {
            const responseCode = parseInt(responseData.substring(0, 3));
            if (responseCode >= 400) {
              reject(EmailReason.DOES_NOT_ACCEPT_MAIL);
            }
          } else {
            reject(EmailReason.IP_BLOCKED);
          }
          return;
        });

        this.socket.once('error', (err) => {
          // Log the error
          this.winstonLoggerService.error(`socket.once() error - ${email}`, JSON.stringify(err));
          // Detect if the connection is blocked
          if (err.message.includes('ECONNREFUSED') || err.message.includes('EHOSTUNREACH')) {
            reject(EmailReason.IP_BLOCKED);
          } else {
            reject(err.message);
          }
          return;
        });

        this.socket.once('timeout', () => {
          // Log the error
          this.winstonLoggerService.error(`socket.once() timeout - ${email}`, responseData);
          resolve(EmailReason.SMTP_TIMEOUT);
          return;
        });
      } catch (e) {
        reject(e);
      }
    });
  }

  async verifyEmail(email: string): Promise<EmailStatusType> {
    // TODO - Must use an email address that belongs to the domain DNS SPF records where the domain is hosted
    const mailFrom = 'tuhin@leadsafeguard.store';
    const [account, domain] = email.split('@');
    return new Promise(async (resolve, reject): Promise<EmailStatusType> => {
      try {
        await this.sendCommand(`EHLO ${this.host}`);
        await this.sendCommand(`MAIL FROM:<${mailFrom}>`);
        // Check for Catch-All email for business domains only.
        // Known Email Providers like gmail.com, yahoo.com, outlook.com do not
        // have catch all.
        if (!freeEmailProviderList.includes(domain)) {
          const catchAllEmail = `${randomStringGenerator()}${Date.now()}@${domain}`;
          const responseCatchAllRcptTo = await this.sendCommand(`RCPT TO:<${catchAllEmail}>`, catchAllEmail);
          const catchAllEmailStatus: EmailStatusType = this.parseSmtpResponseData(responseCatchAllRcptTo, catchAllEmail);
          if (catchAllEmailStatus.status === EmailStatus.VALID) {
            const error: EmailStatusType = {
              status: EmailStatus.CATCH_ALL,
              reason: EmailReason.EMPTY,
            };
            reject(error);
            await this.sendCommand(`QUIT`);
            return;
          }
        }
        const responseRcptTo = await this.sendCommand(`RCPT TO:<${email}>`, email);
        const emailStatus: EmailStatusType = this.parseSmtpResponseData(responseRcptTo, email);
        // console.log({ emailStatus });
        resolve(emailStatus);
        await this.sendCommand(`QUIT`);
      } catch (e) {
        if (typeof e === 'string') {
          if (e === EmailReason.SMTP_TIMEOUT) {
            const error: EmailStatusType = {
              status: EmailStatus.UNKNOWN,
              reason: EmailReason.SMTP_TIMEOUT,
            };
            resolve(error);
            return;
          } else if (e === EmailReason.SOCKET_NOT_FOUND) {
            const error: EmailStatusType = {
              status: EmailStatus.UNKNOWN,
              reason: EmailReason.GREY_LISTED,
            };
            reject(error);
            return;
          }
        }
        const error: EmailStatusType = {
          status: EmailStatus.INVALID,
          reason: e.toString(),
        };

        reject(error);
        await this.sendCommand(`QUIT`);
        return;
      }
    });
  }


  async verifyBulkEmail(bulkFileEmails: BulkFileEmail[]): Promise<EmailValidationResponseType[]> {
    const mailFrom = 'tanimpathan98@gmail.com';
    let emailStatuses: EmailValidationResponseType[] = [];

    await this.sendCommand(`EHLO ${this.host}`);
    await this.sendCommand(`MAIL FROM:<${mailFrom}>`);
    // Check for Catch-All email for business domains only.
    // Known Email Providers like gmail.com, yahoo.com, outlook.com do not
    // have catch all.
    for (const bulkFileEmail of bulkFileEmails) {
      const email = bulkFileEmail.email_address;
      const [account, domain] = email.split('@');
      try {
        const responseRcptTo = await this.sendCommand(`RCPT TO:<${email}>`, email);
        const emailStatus: EmailStatusType = this.parseSmtpResponseData(responseRcptTo, email);
        console.log({ emailStatus });
        emailStatuses.push({
          email_address: email,
          account,
          domain,
          email_status: emailStatus.status,
          email_sub_status: emailStatus.reason,
          retry: emailStatus.retry ? RetryStatus.PENDING : RetryStatus.COMPLETE,
        });
      } catch (e) {
        console.log({ e });
        let errorStatus: EmailStatusType;
        if (typeof e === 'string') {
          if (e === EmailReason.SMTP_TIMEOUT) {
            errorStatus = {
              status: EmailStatus.UNKNOWN,
              reason: EmailReason.SMTP_TIMEOUT,
            };
          } else if (e === EmailReason.SOCKET_NOT_FOUND) {
            errorStatus = {
              status: EmailStatus.UNKNOWN,
              reason: EmailReason.GREY_LISTED,
            };
          } else {
            errorStatus = {
              status: EmailStatus.UNKNOWN,
              reason: e,
            };
          }
        } else {
          errorStatus = {
            status: EmailStatus.INVALID,
            reason: e.toString(),
          };
        }
        console.log({ errorStatus });
        emailStatuses.push({
          email_address: email,
          account,
          domain,
          email_status: errorStatus.status,
          email_sub_status: errorStatus.reason,
        });
      }
    }

    console.log(emailStatuses);
    this.socket.write(`QUIT\r\n`);
    return emailStatuses;
  }

  public parseSmtpResponseData(
    data: string,
    email: string,
  ): EmailStatusType {
    if (data.includes(SMTPResponseCode.TWO_50.smtp_code.toString())) {
      return SMTPResponseCode.TWO_50;
    } else if (data.includes(SMTPResponseCode.TWO_51.smtp_code.toString())) {
      return SMTPResponseCode.TWO_51;
    } else if (
      data.includes(SMTPResponseCode.FIVE_50.smtp_code.toString()) ||
      data.includes(SMTPResponseCode.FIVE_21.smtp_code.toString()) ||
      data.includes(SMTPResponseCode.FIVE_56.smtp_code.toString()) ||
      data.includes(SMTPResponseCode.FIVE_05.smtp_code.toString()) ||
      data.includes(SMTPResponseCode.FIVE_51.smtp_code.toString()) ||
      data.includes(SMTPResponseCode.FIVE_00.smtp_code.toString())
    ) {
      let error: EmailStatusType = { reason: undefined, status: undefined };
      this.winstonLoggerService.error(`(500,556,505,551,550) - ${email}`, data);

      // Check if "data" has any of the strings from 'ipBlockedStringsArray'
      for (const str of ipBlockedStringsArray) {
        if (data.includes(str)) {
          error.status = EmailStatus.SERVICE_UNAVAILABLE;
          error.reason = EmailReason.IP_BLOCKED;
          return error;
        }
      }

      return SMTPResponseCode.FIVE_50;
    } else if (
      // Detect Grey listing (Temporary Failures)
      data.includes(SMTPResponseCode.FOUR_21.smtp_code.toString()) ||
      data.includes(SMTPResponseCode.FOUR_50.smtp_code.toString()) ||
      data.includes(SMTPResponseCode.FOUR_51.smtp_code.toString()) ||
      data.includes(SMTPResponseCode.FOUR_52.smtp_code.toString())
    ) {
      return SMTPResponseCode.FOUR_21;
    } else if (data.includes(SMTPResponseCode.FIVE_53.smtp_code.toString())) {
      return SMTPResponseCode.FIVE_53;
    } else if (data.includes(SMTPResponseCode.FIVE_54.smtp_code.toString())) {
      return SMTPResponseCode.FIVE_54;
    } else {
      let error: EmailStatusType = { reason: undefined, status: undefined };
      // When no other condition is true, handle it for all other codes
      // Response code starts with "4" - Temporary error, and we should retry later
      // Response code starts with "5" - Permanent error and must not retry
      if (data) {
        console.log({ data });
        // Log the response
        if (!data.startsWith('2')) {
          this.winstonLoggerService.error(`parseSmtpResponseData() else - ${email}`, data);
        }

        if (data.startsWith(EmailReason.SMTP_TIMEOUT)) {
          error.status = EmailStatus.UNKNOWN;
          error.reason = EmailReason.SMTP_TIMEOUT;
          return error;
        } else if (data.startsWith(EmailReason.IP_BLOCKED)) {
          return SMTPResponseCode.FIVE_54;
        } else if (data.startsWith('4')) {
          return SMTPResponseCode.FOUR_51;
        } else if (data.startsWith('5')) {
          return SMTPResponseCode.FIVE_50;
        } else {
          error.status = EmailStatus.UNKNOWN;
          error.reason = data;
          return error;
        }
      }
    }

  }
}
