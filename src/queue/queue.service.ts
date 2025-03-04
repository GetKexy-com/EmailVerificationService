import { Injectable } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bull';
import { JobOptions, Queue } from 'bull';
import {
  GREY_LIST_MINUTE_GAP,
  PROCESS_BULK_FILE_QUEUE,
  PROCESS_EMAIL_SEND_QUEUE,
  PROCESS_GREY_LIST_QUEUE,
  QUEUE,
} from '@/common/utility/constant';
import { MailerService } from '@/mailer/mailer.service';
import {
  EmailReason,
  EmailStatus,
  EmailStatusType,
  EmailValidationResponseType,
  SendMailOptions,
} from '@/common/utility/email-status-type';
import Bottleneck from 'bottleneck';
import { Domain, MXRecord } from '@/domains/entities/domain.entity';
import { DomainService } from '@/domains/services/domain.service';
import { WinstonLoggerService } from '@/logger/winston-logger.service';
import { SmtpConnectionService } from '@/smtp-connection/smtp-connection.service';
import { BulkFile, BulkFileStatus } from '@/bulk-files/entities/bulk-file.entity';
import { BulkFileEmailsService } from '@/bulk-file-emails/bulk-file-emails.service';

@Injectable()
export class QueueService {
  constructor(
    private readonly mailerService: MailerService,
    private readonly domainService: DomainService,
    private readonly smtpService: SmtpConnectionService,
    private readonly bulkFileEmailsService: BulkFileEmailsService,
    private readonly winstonLoggerService: WinstonLoggerService,
    @InjectQueue(QUEUE) private readonly queue: Queue,
  ) {
  }

  // Bottleneck for rate limiting (CommonJS compatible)
  public limiter = new Bottleneck({
    maxConcurrent: 1, // Adjust based on your testing
    minTime: 300, // 200ms delay between requests (adjustable)
  });


  async addGreyListEmailToQueue(emailSmtpResponses: EmailValidationResponseType[], bulkFile: BulkFile) {
    const jobOptions: JobOptions = {
      attempts: 1, // Retry 3 times if failed
      delay: GREY_LIST_MINUTE_GAP, // delay for 15 minutes
      removeOnComplete: true, // Automatically delete job after processing
    };
    const jobData = {
      bulkFile,
      emailSmtpResponses,
    };
    await this.queue.add(PROCESS_GREY_LIST_QUEUE, jobData, jobOptions);
    console.log('Done Adding to Grey list');
  }

  async addBulkFileToQueue(bulkFile: BulkFile) {
    const jobOptions: JobOptions = {
      attempts: 1, // Retry 3 times if failed
      delay: 0,
      removeOnComplete: true, // Automatically delete job after processing
    };
    await this.queue.add(PROCESS_BULK_FILE_QUEUE, bulkFile, jobOptions);
  }

  async addEmailToQueue(data: any) {
    const jobOptions: JobOptions = {
      attempts: 3, // Retry 3 times if failed
      delay: 0,
      removeOnComplete: true, // Automatically delete job after processing
    };
    await this.queue.add(PROCESS_EMAIL_SEND_QUEUE, data, jobOptions);
  }

  async processEmailQueue(emailData: SendMailOptions) {
    return this.mailerService.sendEmail(emailData);
  }

  async runGrayListCheck(emailQueueData: any) {
    // Bottleneck for rate limiting (CommonJS compatible)
    const limiter = this.limiter;

    const emailSmtpResponses: EmailValidationResponseType[] = emailQueueData.emailSmtpResponses;
    const bulkFile: BulkFile = emailQueueData.bulkFile;
    console.log(emailSmtpResponses);

    const validationPromises: Promise<any>[] = emailSmtpResponses.map((greyEmailRedisResponse: EmailValidationResponseType) => limiter.schedule(async () => {
      console.log(`Gray Verify ${greyEmailRedisResponse.email_address} started`);
      const domain: Domain = await this.domainService.findOne(greyEmailRedisResponse.domain);
      let newEmailStatus: EmailStatusType;
      try {
        const allMxRecordHost: MXRecord[] = JSON.parse(domain.mx_record_hosts);
        const index = Math.floor(Math.random() * allMxRecordHost.length);
        const mxRecordHost = allMxRecordHost[index].exchange;
        // Here we are not creating new instance of 'SmtpConnectionService' because,
        // We run only 1 connection at a time through 'Bottleneck'. So we can reuse
        // the same socket without crossing the max socket connection limit which is 10
        await this.smtpService.connect(mxRecordHost);
        newEmailStatus = await this.smtpService.verifyEmail(greyEmailRedisResponse.email_address);
      } catch (e) {
        newEmailStatus = e;
        this.winstonLoggerService.error('Gray List Error', e);
      }
      // If email status is still GREY_LISTED then mark it invalid.
      if (newEmailStatus.reason === EmailReason.GREY_LISTED) {
        greyEmailRedisResponse.email_status = EmailStatus.INVALID;
        greyEmailRedisResponse.email_sub_status = EmailReason.MAILBOX_NOT_FOUND;
      } else {
        greyEmailRedisResponse.email_status = newEmailStatus.status;
        greyEmailRedisResponse.email_sub_status = newEmailStatus.reason;
      }
      console.log({ emailStatus: newEmailStatus });
      // greyEmailRedisResponse.retry = RetryStatus.COMPLETE;
      await this.domainService.updateProcessedEmailByEmail(greyEmailRedisResponse.email_address, greyEmailRedisResponse);

      return newEmailStatus;
    }));

    // Wait for all validations to complete
    await Promise.allSettled(validationPromises);
    console.log('Updating bulk file status');
    // TODO - Update file status in BulkFile
    const dbBulkFile = await BulkFile.findOneBy({ id: bulkFile.id });
    dbBulkFile.file_status = BulkFileStatus.GREY_LIST_CHECK_DONE;
    await dbBulkFile.save();
    console.log(`Bulk file status updated for ${bulkFile.id}`);
  }

  async saveBulkFileEmails(bulkFile: BulkFile) {
    await this.bulkFileEmailsService.saveBulkFileEmails(bulkFile);
  }

}
