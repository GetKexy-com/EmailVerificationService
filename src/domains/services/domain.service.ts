import { HttpException, HttpStatus, Injectable, NotFoundException } from '@nestjs/common';
import * as dns from 'dns';
import { DataSource } from 'typeorm';

import { PaginationQueryDto } from '@/common/dto/pagination-query.dto';
import {
  CATCH_ALL_CHECK_DAY_GAP,
  ERROR_DOMAIN_CHECK_DAY_GAP,
  MX_RECORD_CHECK_DAY_GAP,
  SPAM_DB_CHECK_DAY_GAP,
} from '@/common/utility/constant';
import { DNSBL } from '@/common/utility/dnsbl';
import DomainTypoChecker from '@/common/utility/domain-typo-checker';
import {
  EmailReason,
  EmailStatus,
  EmailStatusType,
  EmailValidationResponseType,
  SendMailOptions,
} from '@/common/utility/email-status-type';
import freeEmailProviderList from '@/common/utility/free-email-provider-list';
import { DisposableDomainsService } from '@/disposable-domains/disposable-domains.service';
import { EmailRolesService } from '@/email-roles/email-roles.service';
import { EmailRole } from '@/email-roles/entities/email-role.entity';
import { CreateDomainDto } from '@/domains/dto/create-domain.dto';
import { UpdateDomainDto } from '@/domains/dto/update-domain.dto';
import { Domain, MXRecord } from '@/domains/entities/domain.entity';
import { ErrorDomain } from '@/domains/entities/error_domain.entity';
import { ProcessedEmail, RetryStatus } from '@/domains/entities/processed_email.entity';
import { WinstonLoggerService } from '@/logger/winston-logger.service';
import { User } from '@/users/entities/user.entity';
import { MailerService } from '@/mailer/mailer.service';
import { ConfigService } from '@nestjs/config';
import { SmtpConnectionService } from '@/smtp-connection/smtp-connection.service';
import { TimeService } from '@/time/time.service';
import { BulkFileEmail } from '@/bulk-file-emails/entities/bulk-file-email.entity';

@Injectable()
export class DomainService {
  constructor(
    private dataSource: DataSource,
    private configService: ConfigService,
    private disposableDomainsService: DisposableDomainsService,
    private emailRolesService: EmailRolesService,
    private mailerService: MailerService,
    private smtpService: SmtpConnectionService,
    private timeService: TimeService,
    private winstonLoggerService: WinstonLoggerService,
  ) {
  }

  async createMany(domains: Domain[]) {
    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();
    try {
      await queryRunner.manager.save(Domain, domains);
      await queryRunner.commitTransaction();
    } catch (err) {
      // since we have errors lets rollback the changes we made
      await queryRunner.rollbackTransaction();
      throw new HttpException(err.message, HttpStatus.BAD_REQUEST);
    } finally {
      // you need to release a queryRunner which was manually instantiated
      await queryRunner.release();
    }
  }

  findAll(paginationQuery: PaginationQueryDto) {
    const { limit, offset } = paginationQuery;
    return Domain.find({
      skip: offset,
      take: limit,
    });
  }

  async findOne(domain: string) {
    return Domain.findOneBy({ domain });
  }

  async findErrorDomain(domain: string): Promise<ErrorDomain> {
    return new Promise(async (resolve: any, reject): Promise<ErrorDomain> => {
      // Check if domain is listed in error_domains and
      const errorDomain: ErrorDomain = await ErrorDomain.findOneBy({
        domain,
      });
      if (errorDomain) {
        const errorStatus: string = errorDomain.domain_error['status'];
        const dayPassedSinceCheck = this.timeService.getTimeDifferenceInDays(
          new Date(),
          errorDomain.created_at,
        );
        // if the error is 'spamtrap' and if error listed time
        // is not more than SPAM_DB_CHECK_DAY_GAP then we SKIP checking again
        if (errorStatus === EmailStatus.SPAMTRAP) {
          if (dayPassedSinceCheck < SPAM_DB_CHECK_DAY_GAP) {
            reject(errorDomain.domain_error);
            return;
          }
          // if the error is 'spamtrap' and if error listed time
          // is not more than CATCH_ALL_CHECK_DAY_GAP then we SKIP checking again
        } else if (errorStatus === EmailStatus.CATCH_ALL) {
          if (dayPassedSinceCheck < CATCH_ALL_CHECK_DAY_GAP) {
            reject(errorDomain.domain_error);
            return;
          }
        } else {
          if (dayPassedSinceCheck < ERROR_DOMAIN_CHECK_DAY_GAP) {
            reject(errorDomain.domain_error);
            return;
          }
        }
      }
      resolve(errorDomain);
    });
  }

  async saveProcessedEmail(processedEmail: EmailValidationResponseType, user, bulkFileId = null) {
    const existingDomain = await ProcessedEmail.findOneBy({
      email_address: processedEmail.email_address,
    });
    if (!existingDomain) {
      const dbProcessedEmail: ProcessedEmail = ProcessedEmail.create({
        ...processedEmail,
        user_id: user.id,
        bulk_file_id: bulkFileId,
      });
      return dbProcessedEmail.save();
    }
  }

  async getCachedProcessedEmail(email: string) {
    const processedEmail: ProcessedEmail = await ProcessedEmail.findOneBy({
      email_address: email,
    });
    if (processedEmail) {
      if (this.timeService.shouldReturnCachedProcessedEmail(processedEmail)) {
        return processedEmail;
      }
    }
    return null;
  }


  async getProcessedEmail(email: string) {
    const processedEmail: ProcessedEmail = await ProcessedEmail.findOneBy({
      email_address: email,
    });
    if (processedEmail) {
      return processedEmail;
    }
    return null;
  }


  async getGreyListedProcessedEmail(bulkFileId: number) {
    return ProcessedEmail.find({
      where: [
        {
          email_sub_status: EmailReason.GREY_LISTED,
          bulk_file_id: bulkFileId,
          retry: RetryStatus.PENDING,
        },
      ],
      order: {
        id: 'ASC',
      },
    });
  }

  async findProcessedEmailsByFileId(fileId: number) {
    return ProcessedEmail.find({
      where: { bulk_file_id: fileId },
      order: { id: 'ASC' },
    });
  }

  async create(createDomainDto: CreateDomainDto) {
    const existingDomain = await Domain.findOneBy({
      domain: createDomainDto.domain,
    });
    if (existingDomain) {
      throw new HttpException(
        `Domain ${createDomainDto.domain} already exist`,
        HttpStatus.FOUND,
      );
    }
    const domain = Domain.create({ ...createDomainDto });
    return domain.save();
  }

  async createOrUpdateErrorDomain(errorDomain: ErrorDomain) {
    const existingDomain = await ErrorDomain.findOneBy({
      domain: errorDomain.domain,
    });
    if (existingDomain) {
      if (
        errorDomain.domain_error['status'] !==
        existingDomain.domain_error['status']
      ) {
        return true;
      } else {
        existingDomain.domain_error = errorDomain.domain_error;
        await existingDomain.save();
        return true;
      }
    }
    const domain = ErrorDomain.create({ ...errorDomain });
    return domain.save();
  }

  async update(domain: string, updateDto: UpdateDomainDto) {
    const existingDomain = await Domain.findOneBy({ domain });
    if (!existingDomain) {
      throw new NotFoundException(`Domain #${domain} not found`);
    }
    if (updateDto.domain) delete updateDto.domain;
    const updatedDomain = { ...existingDomain, ...updateDto };
    try {
      await Domain.update(domain, updatedDomain);
      return updatedDomain;
    } catch (e) {
      throw new HttpException(e, HttpStatus.BAD_REQUEST);
    }
  }

  async updateProcessedEmail(processedEmailId: number, data) {
    const existingDomain = await ProcessedEmail.findOne({ where: { id: processedEmailId } });
    const updatedData = { ...existingDomain, ...data };
    await ProcessedEmail.update(processedEmailId, updatedData);
  }

  async updateProcessedEmailByEmail(email: string, data) {
    const existingDomain = await ProcessedEmail.findOne({ where: { email_address: email } });
    if (existingDomain) {
      const updatedData = { ...existingDomain, ...data };
      await ProcessedEmail.update(existingDomain.id, updatedData);
    }
  }

  async remove(domainRemove: string) {
    const domain = await Domain.findOneBy({ domain: domainRemove });
    return domain.remove();
  }

  // Validate email syntax
  async validateEmailFormat(email) {
    return new Promise((resolve, reject) => {
      const emailRegex = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
      const isValid = emailRegex.test(email);
      if (!isValid) {
        const error: EmailStatusType = {
          status: EmailStatus.INVALID,
          reason: EmailReason.INVALID_EMAIL_FORMAT,
        };
        reject(error);
        return;
      }
      resolve(true);
    });
  }

  async checkDomainMxRecords(
    domain: string,
    dbDomain: Domain,
  ): Promise<any> {
    return new Promise(async (resolve, reject) => {
      // Check if mx record saveBulkFile time is pass 30 days or not.
      // If yes - then revalidate mx records to make sure it is still valid
      // If not - then continue using it
      if (dbDomain) {
        const dayPassedSinceLastMxCheck = this.timeService.getTimeDifferenceInDays(
          new Date(),
          dbDomain.created_at,
        );
        if (dayPassedSinceLastMxCheck < MX_RECORD_CHECK_DAY_GAP) {
          resolve(JSON.parse(dbDomain.mx_record_hosts));
          return;
        }
      }

      // TODO - These 2 check takes more resources and disabled for now
      // const aRecords = await dns.promises.resolve(domain, 'A').catch(() => []);
      // const aaaaRecords = await dns.promises.resolve(domain, 'AAAA').catch(() => []);
      const mxRecords = await dns.promises.resolveMx(domain).catch(() => []);
      // TODO - These check takes more resources and disabled for now
      // If all records are empty, no DNS entries exist
      // if (aRecords.length === 0 && aaaaRecords.length === 0 && mxRecords.length === 0) {
      //    reject({ status: 'invalid', reason: 'no_dns_entries' });
      // }

      // TODO - These 2 check takes more resources and disabled for now
      // If all records are empty, no DNS entries exist
      // if (aRecords.length !== 0 && aaaaRecords.length !== 0 && mxRecords.length === 0) {
      if (mxRecords.length === 0) {
        const error: EmailStatusType = {
          status: EmailStatus.INVALID,
          reason: EmailReason.NO_MX_FOUND,
        };
        reject(error);

        return;
      }

      mxRecords.sort((a, b) => a.priority - b.priority);

      // If the domain is already saved but mx_record is new then we update our record
      if (dbDomain) {
        dbDomain.mx_record_hosts = JSON.stringify(mxRecords);
        await dbDomain.save();
      }

      resolve(mxRecords);
    });
  }


  checkDomainSpamDatabaseList(domain: string) {
    return new Promise((resolve, reject) => {
      const dnsbl = new DNSBL(domain);

      dnsbl.on('error', function(error, blocklist) {
      });
      dnsbl.on('data', async function(result, blocklist) {
        if (result.status === 'listed') {
          const error: EmailStatusType = {
            status: EmailStatus.SPAMTRAP,
            reason: EmailReason.EMPTY,
          };
          reject(error);
          return;
        }
      });
      dnsbl.on('done', function() {
        resolve(true);
      });
    });
  }

  async isRoleBasedEmail(email) {
    return new Promise(async (resolve, reject) => {
      const localPart = email.split('@')[0].toLowerCase();
      const isRoleBased: EmailRole =
        await this.emailRolesService.findOne(localPart);
      if (isRoleBased) {
        const error: EmailStatusType = {
          status: EmailStatus.DO_NOT_MAIL,
          reason: EmailReason.ROLE_BASED,
        };
        reject(error);
        return;
      }
      resolve(true);
    });
  }

  async isDisposableDomain(domain) {
    return new Promise(async (resolve, reject) => {
      const isDisposable =
        await this.disposableDomainsService.findByDomain(domain);
      if (isDisposable) {
        const error: EmailStatusType = {
          status: EmailStatus.DO_NOT_MAIL,
          reason: EmailReason.DISPOSABLE_DOMAIN,
        };
        reject(error);
        return;
      }
      resolve(true);
    });
  }

  async domainTypoCheck(domain) {
    return new Promise((resolve, reject) => {
      const domainHasTypo = new DomainTypoChecker().check(domain);
      if (domainHasTypo) {
        const error: EmailStatusType = {
          status: EmailStatus.INVALID,
          reason: EmailReason.POSSIBLE_TYPO,
        };
        reject(error);
        return;
      }
      resolve(true);
    });
  }

  async bulkOutlookEmailVerification(emails: BulkFileEmail[], mxRecordHost: string, user: User, bulkFileId: number) {
    let smtpService: SmtpConnectionService;
    let smtpConnectionStatus: EmailStatusType;
    smtpService = new SmtpConnectionService(this.winstonLoggerService);
    smtpConnectionStatus = await smtpService.connect(mxRecordHost);
    const emailStatuses: EmailValidationResponseType[] = await smtpService.verifyBulkEmail(emails);
    if (emailStatuses.length) {
      for (const emailStatus of emailStatuses) {
        if (emailStatus.email_sub_status !== EmailReason.GREY_LISTED) {
          emailStatus.retry = RetryStatus.COMPLETE;
        } else {
          emailStatus.retry = RetryStatus.PENDING;
        }
        emailStatus.free_email = false;
        await this.saveProcessedEmail(emailStatus, user, bulkFileId);
      }
    }
    return emailStatuses;
  }

  async smtpValidation(email: string, user: User, bulkFileId = null) {
    let emailStatus: EmailValidationResponseType = {
      email_address: email,
      verify_plus: false,
    };

    // Check if we processed the email within allowed time/day and emails is
    // VALID || CATCH_ALL || SPAMTRAP || DO_NOT_MAIL.
    // If so, return the previous cached result.
    // Otherwise, re-run the validation again
    const processedEmail: ProcessedEmail = await this.getCachedProcessedEmail(email);
    if (processedEmail) {
      // Delete these property so these are not included in the final response.
      delete processedEmail.id;
      delete processedEmail.user_id;
      delete processedEmail.bulk_file_id;
      delete processedEmail.created_at;
      delete processedEmail.retry;
      emailStatus = { ...emailStatus, ...processedEmail };
      // console.log(`${email}`, { processedEmail });
      return emailStatus;
    }

    const [account, domain] = email.split('@');
    // Get domain part from the email address
    emailStatus.account = account;
    emailStatus.domain = domain;

    try {
      // Step - 1 : Check email syntax validity
      await this.validateEmailFormat(email);

      // Query DB to check if domain found in error_domains
      // If domain is listed as ErrorDomain then we check if enough time passed to recheck
      // otherwise, we reject from there, and it is catch in the try ...catch block
      // So we do not check the response from findErrorDomain here.
      await this.findErrorDomain(domain);

      // Query DB for existing domain check
      let dbDomain: Domain = await this.findOne(domain);

      // Check if domain is one of free email providers.
      // If Yes - We SKIP,
      // skip role based check,
      // domain disposable check,
      // black list check,
      // domain typo check and
      // catch-all domain check.
      const isFreeEmailDomain = freeEmailProviderList.includes(domain);
      emailStatus.free_email = isFreeEmailDomain;
      if (!isFreeEmailDomain) {
        // Step - 2 : Check email is role based
        // Ex - contact@domain.com
        // We mark these as 'role_based' as these emails might be valid but
        // high chance of not getting any reply back.
        await this.isRoleBasedEmail(email);

        // Step - 3 : Check email is a temporary email
        await this.isDisposableDomain(domain);

        // Step - 4 : Check if the domain is one of DNSBL domain
        // Hint - Domain Name System Blacklists, also known as DNSBL's or DNS Blacklists,
        // are spam blocking lists. They allow a website administrator to block
        // messages from specific systems that have a history of sending spam.
        // These lists are based on the Internet's Domain Name System, or DNS.
        await this.checkDomainSpamDatabaseList(domain);

        // Step - 5 : Check if the domain name is very similar to another popular domain
        // Usually these domains are used for spam or spam-trap.
        await this.domainTypoCheck(domain);
      }

      // Step 7 : Get the MX records of the domain
      const allMxRecordHost: MXRecord[] = await this.checkDomainMxRecords(
        domain,
        dbDomain,
      );

      // Get the 0 index mxHost as it has the highest priority.
      // We sort the MX records by their priority in ASC order
      const mxRecordHost = allMxRecordHost[0].exchange;
      // Save The domain if it is not already saved
      if (!dbDomain) {
        const createDomainDto: CreateDomainDto = {
          domain,
          domain_age_days: null,
          mx_record_hosts: JSON.stringify(allMxRecordHost),
          domain_ip: '',
        };
        await this.create(createDomainDto);
      }

      // Step 9 : Make a SMTP Handshake to very if the email address exist in the mail server
      // If email exist then we can confirm the email is valid
      // const smtpResponse: EmailStatusType = await this.verifySmtp(
      //   email,
      //   mxRecordHost,
      // );
      console.log({ email });
      let smtpService: SmtpConnectionService;
      let smtpConnectionStatus: EmailStatusType;
      // try {
      smtpService = new SmtpConnectionService(this.winstonLoggerService);
      smtpConnectionStatus = await smtpService.connect(mxRecordHost);
      // } catch (e) {
      // e - is type of 'EmailStatusType' as we reject with
      // this type from SmtpConnectionService connect().
      // That's how we know we can assign 'e' to 'smtpConnectionStatus'
      // smtpConnectionStatus = e;
      // }
      // When 'SMTP_TIMEOUT', we resolve it to process here. Otherwise,
      // rejection occur, and it goes to catch block
      // If - User enabled verify+ and smtp response
      // is a 'timeout' then we must trigger Verify+
      if (smtpConnectionStatus.reason === EmailReason.SMTP_TIMEOUT) {
        emailStatus = await this.__processVerifyPlus(email, user, smtpConnectionStatus, emailStatus);
      } else {
        const smtpResponse: EmailStatusType = await smtpService.verifyEmail(email);
        emailStatus = await this.__processVerifyPlus(email, user, smtpResponse, emailStatus);
      }

      // Step - 6 : Check domain whois database to make sure everything is in good shape
      if (emailStatus.email_sub_status !== EmailReason.GREY_LISTED) {
        // const domainInfo: any = await this.getDomainAge(domain, dbDomain);
        // dbDomain.domain_age_days = domainInfo.domain_age_days;
        // await dbDomain.save();

        // emailStatus.domain_age_days = domainInfo.domain_age_days;
        emailStatus.retry = RetryStatus.COMPLETE;
      } else {
        emailStatus.retry = RetryStatus.PENDING;
      }
      await this.saveProcessedEmail(emailStatus, user, bulkFileId);
      // If everything goes well, then return the emailStatus
      return emailStatus;
    } catch (error) {
      emailStatus.email_status = error['status'];
      emailStatus.email_sub_status = error['reason'];
      emailStatus.free_email = freeEmailProviderList.includes(
        emailStatus.domain,
      );
      await this.saveProcessedErrorEmail(emailStatus, error, email, user, bulkFileId);

      return emailStatus;
    }
  }

  private async __processVerifyPlus(
    email: string,
    user: User,
    smtpResponse: EmailStatusType,
    emailOldStatus: EmailValidationResponseType,
  ): Promise<EmailValidationResponseType> {
    const emailStatus: EmailValidationResponseType = emailOldStatus;
    if (user.verify_plus && smtpResponse.reason === EmailReason.SMTP_TIMEOUT) {
      const verifyPlusResponse: EmailStatusType = await this.__sendVerifyPlusEmail(email);
      emailStatus.email_status = verifyPlusResponse.status;
      emailStatus.email_sub_status = verifyPlusResponse.reason;
      emailStatus.verify_plus = true;
      return emailStatus;
    } else {
      emailStatus.email_status = smtpResponse.status;
      emailStatus.email_sub_status = smtpResponse.reason;
      emailStatus.verify_plus = false;
      return emailStatus;
    }
  }

  private async __sendVerifyPlusEmail(email: string) {
    const emailData: SendMailOptions = {
      fromEmail: this.configService.get<string>('VERIFY_PLUS_FROM_EMAIL'),
      to: email,
      headers: {
        // This header allow us tracking email delivery status.
        'X-SES-CONFIGURATION-SET': 'Default',
      },
      subject: '',
      // template name is required even if its empty.
      template: 'email_verify+',
      context: {},
      attachments: [],
    };
    const emailResponse = await this.mailerService.sendEmail(emailData);
    console.log('Verify+');
    console.log({ emailResponse });
    return this.smtpService.parseSmtpResponseData(emailResponse.response, email);
  }

  async saveProcessedErrorEmail(
    emailStatus: EmailValidationResponseType,
    error: { reason: EmailReason; },
    email: string,
    user: User,
    bulkFileId: number,
  ) {
    try {
      await this.saveProcessedEmail(emailStatus, user, bulkFileId);
      // If the email is a free email OR Error reasons
      // DO NOT confirm the domain has issues. Other emails
      // from the same domain might be valid.
      // So we do not save the domain into error_domains
      const skipReasons = [
        EmailReason.ROLE_BASED,
        EmailReason.INVALID_EMAIL_FORMAT,
        EmailReason.UNVERIFIABLE_EMAIL,
        EmailReason.MAILBOX_NOT_FOUND,
        EmailReason.IP_BLOCKED,
        EmailReason.SMTP_TIMEOUT,
      ];
      if (
        emailStatus.free_email ||
        (error.reason && skipReasons.includes(error.reason))
      ) {
        return emailStatus;
      }

      const domain = email.split('@')[1];
      const errorDomain: any = {
        domain,
        domain_error: error,
      };
      await this.createOrUpdateErrorDomain(errorDomain);
    } catch (err) {
      this.winstonLoggerService.error(`saveProcessedErrorEmail() - ${email}`, err);

      return err;
    }
  }
}
