import { Injectable } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';

import { BulkFilesService } from '@/bulk-files/bulk-files.service';
import { UpdateBulkFileDto } from '@/bulk-files/dto/update-bulk-file.dto';
import { BulkFile, BulkFileStatus } from '@/bulk-files/entities/bulk-file.entity';
import {
  EmailReason,
  EmailStatus,
  EmailValidationResponseType,
  SendMailOptions,
} from '@/common/utility/email-status-type';
import { DomainService } from '@/domains/services/domain.service';
import { WinstonLoggerService } from '@/logger/winston-logger.service';
import Bottleneck from 'bottleneck';
import { UsersService } from '@/users/users.service';
import { User } from '@/users/entities/user.entity';
import { ProcessedEmail } from '@/domains/entities/processed_email.entity';
import { QueueService } from '@/queue/queue.service';
import { Attachment } from 'nodemailer/lib/mailer';
import { BulkFileEmailsService } from '@/bulk-file-emails/bulk-file-emails.service';
import { BulkFileEmail } from '@/bulk-file-emails/entities/bulk-file-email.entity';
import * as path from 'path';
import { DEV, MicrosoftDomains } from '@/common/utility/constant';
import * as process from 'process';

@Injectable()
export class SchedulerService {

  constructor(
    private bulkFilesService: BulkFilesService,
    private bulkFileEmailsService: BulkFileEmailsService,
    private domainService: DomainService,
    private userService: UsersService,
    private queueService: QueueService,
    private winstonLoggerService: WinstonLoggerService,
  ) {
  }

  @Cron(CronExpression.EVERY_MINUTE)
  public async runFileEmailValidation() {
    const pendingFiles: BulkFile[] = await this.bulkFilesService.getPendingBulkFile();
    console.log({ pendingFiles });
    if (!pendingFiles.length) {
      return;
    }
    const bulkFile: BulkFile = pendingFiles[0];
    const user: User = await this.userService.findOneById(bulkFile.user_id);
    if (!user) {
      this.winstonLoggerService.error('runFileEmailValidation()', `No user found for user_id: ${bulkFile.user_id}`);

      return;
    }
    try {
      const processingStatus: UpdateBulkFileDto = {
        file_status: BulkFileStatus.PROCESSING,
      };
      await this.bulkFilesService.updateBulkFile(
        bulkFile.id,
        processingStatus,
      );

      await this.__bulkValidate(bulkFile, user);

      const bulkFileUpdateData: UpdateBulkFileDto = {
        file_status: BulkFileStatus.READY_FOR_GREY_LIST,
      };
      await this.bulkFilesService.updateBulkFile(
        bulkFile.id,
        bulkFileUpdateData,
      );

    } catch (e) {
      this.winstonLoggerService.error('Bulk File Error', e.trace);
      console.log(e);
    }
  }

  @Cron(CronExpression.EVERY_MINUTE)
  public async processGreyListBulkFile() {
    const greyListFile = await this.bulkFilesService.getGreyListReadyBulkFile();
    if (!greyListFile.length) {
      return;
    }
    const bulkFile: BulkFile = greyListFile[0];
    const user: User = await this.userService.findOneById(bulkFile.user_id);
    if (!user) {
      this.winstonLoggerService.error('processGreyListBulkFile()', `No user found for user_id: ${bulkFile.user_id}`);

      return;
    }

    const bulkFileEmails: BulkFileEmail[] = await this.bulkFileEmailsService.findBulkFileEmails(bulkFile.id);
    const greyListEmails: EmailValidationResponseType[] = [];
    for (const bulkFileEmail of bulkFileEmails) {
      const email: ProcessedEmail = await this.domainService.getProcessedEmail(bulkFileEmail.email_address);

      if (email && email.email_sub_status && email.email_sub_status === EmailReason.GREY_LISTED) {
        greyListEmails.push(email);
      }
    }
    if (greyListEmails.length) {
      console.log(`Adding to Grey list ${greyListEmails.length} emails`);
      await this.queueService.addGreyListEmailToQueue(greyListEmails, bulkFile);
    }

    const bulkFileUpdateData: UpdateBulkFileDto = {
      file_status: greyListEmails.length ? BulkFileStatus.GREY_LIST_CHECK_PROGRESS : BulkFileStatus.GREY_LIST_CHECK_DONE,
    };
    await this.bulkFilesService.updateBulkFile(
      bulkFile.id,
      bulkFileUpdateData,
    );

  }

  @Cron(CronExpression.EVERY_MINUTE)
  public async generateCsvAndSendEmailForGreyListCheckedFiles() {
    const greyListFile = await this.bulkFilesService.getGreyListCheckBulkFile();
    if (!greyListFile.length) {
      return;
    }
    const bulkFile: BulkFile = greyListFile[0];
    const user: User = await this.userService.findOneById(bulkFile.user_id);
    if (!user) {
      this.winstonLoggerService.error('runGrayListEmailValidation()', `No user found for user_id: ${bulkFile.user_id}`);

      return;
    }

    // Generate all csv and update DB with updated counts.
    const folderName: string = bulkFile.file_path.split('/').at(-1).replace('.csv', '');
    const uploadPath: string = process.env.NODE_ENV === DEV ? '/uploads' : '../uploads';
    const csvSavePath = path.join(process.cwd(), uploadPath, 'csv', 'validated', folderName);

    const {
      valid_email_count,
      invalid_email_count,
      unknown_count,
      catch_all_count,
      do_not_mail_count,
      spam_trap_count,
    } = await this.__generateBulkFileResultCsv(bulkFile.id, folderName);

    let completeStatus = {
      file_status: BulkFileStatus.COMPLETE,
      validation_file_path: csvSavePath,
      valid_email_count,
      invalid_email_count,
      unknown_count,
      catch_all_count,
      do_not_mail_count,
      spam_trap_count,
      updated_at: new Date(),
    };
    await this.bulkFilesService.updateBulkFile(
      bulkFile.id,
      completeStatus,
    );

    await this.__sendEmailNotification(user, bulkFile.id);
  }

  private async __generateBulkFileResultCsv(fileId: number, folderName: string) {
    const bulkFile: BulkFile = await this.bulkFilesService.getBulkFile(fileId);
    const results = await this.__readSCsvAndMergeValidationResults(bulkFile.file_path);
    await this.__saveValidationResultsInCsv(results, folderName);
    return this.__getValidationsByTypes(results);
  }


  private async __sendEmailNotification(user: User, bulkFileId: number) {
    const bulkFile: BulkFile = await this.bulkFilesService.getBulkFile(bulkFileId);
    const to = `${user.first_name} ${user.last_name} <${user.email_address}>`;
    const attachments: Attachment[] = [];
    // Get all csv files from the 'csvSavePath to send as email attachment
    const csvFiles = await this.bulkFilesService.__getAllFilesInFolder(bulkFile.validation_file_path);
    if (csvFiles.length) {
      csvFiles.forEach(file => {
        attachments.push({
          path: file,
        });
      });
    }
    const emailDynamicData = {
      user,
      bulkFile,
      downloadLink: 'https://leadwrap.com/downlaod/',
      unsubscribeLink: 'https://leadwrap.com/unsubscribe/',
    };
    const emailData: SendMailOptions = {
      to,
      subject: 'LearWrap Email Verification is Complete',
      template: 'email_verification_complete',
      context: emailDynamicData,
      attachments,
      bcc: ['arahimdu50@gmail.com']
    };
    await this.queueService.addEmailToQueue(emailData);
  }

  private async __saveValidationResultsInCsv(results: EmailValidationResponseType[], folderName: string) {
    const fileWithStatusTypes = {
      [EmailReason.ROLE_BASED]: [],
      [EmailReason.UNVERIFIABLE_EMAIL]: [],
      [EmailReason.DISPOSABLE_DOMAIN]: [],
      [EmailReason.MAILBOX_NOT_FOUND]: [],
      [EmailReason.DOMAIN_NOT_FOUND]: [],
      [EmailReason.SMTP_TIMEOUT]: [],
      [EmailReason.DOES_NOT_ACCEPT_MAIL]: [],
      [EmailReason.IP_BLOCKED]: [],
      [EmailReason.GREY_LISTED]: [],
      [EmailStatus.CATCH_ALL]: [],
      [EmailStatus.SPAMTRAP]: [],
      [EmailStatus.VALID]: [],
    };

    results.forEach((email: EmailValidationResponseType) => {
      if (email.email_status === EmailStatus.VALID) {
        fileWithStatusTypes[EmailStatus.VALID].push(email);
      } else if (email.email_status === EmailStatus.CATCH_ALL) {
        fileWithStatusTypes[EmailStatus.CATCH_ALL].push(email);
      } else if (email.email_sub_status === EmailReason.GREY_LISTED) {
        fileWithStatusTypes[EmailReason.GREY_LISTED].push(email);
      } else if (email.email_status === EmailStatus.SPAMTRAP) {
        fileWithStatusTypes[EmailStatus.SPAMTRAP].push(email);
      } else if (email.email_sub_status === EmailReason.ROLE_BASED) {
        fileWithStatusTypes[EmailReason.ROLE_BASED].push(email);
      } else if (email.email_sub_status === EmailReason.UNVERIFIABLE_EMAIL) {
        fileWithStatusTypes[EmailReason.UNVERIFIABLE_EMAIL].push(email);
      } else if (email.email_sub_status === EmailReason.DISPOSABLE_DOMAIN) {
        fileWithStatusTypes[EmailReason.DISPOSABLE_DOMAIN].push(email);
      } else if (email.email_sub_status === EmailReason.MAILBOX_NOT_FOUND) {
        fileWithStatusTypes[EmailReason.MAILBOX_NOT_FOUND].push(email);
      } else if (email.email_sub_status === EmailReason.DOMAIN_NOT_FOUND) {
        fileWithStatusTypes[EmailReason.DOMAIN_NOT_FOUND].push(email);
      } else if (email.email_sub_status === EmailReason.SMTP_TIMEOUT) {
        fileWithStatusTypes[EmailReason.SMTP_TIMEOUT].push(email);
      } else if (email.email_sub_status === EmailReason.IP_BLOCKED) {
        fileWithStatusTypes[EmailReason.IP_BLOCKED].push(email);
      } else if (email.email_sub_status === EmailReason.DOES_NOT_ACCEPT_MAIL) {
        fileWithStatusTypes[EmailReason.DOES_NOT_ACCEPT_MAIL].push(email);
      }
    });

    for (const fileType of Object.keys(fileWithStatusTypes)) {
      const fileName = folderName + '/' + fileType + '.csv';
      const csvData: [] = fileWithStatusTypes[fileType];
      if (csvData.length) {
        await this.bulkFilesService.generateCsv(
          csvData,
          fileName,
        );
        console.log(`${fileName} created`);
      }
    }

    // All data in one file.
    await this.bulkFilesService.generateCsv(
      results,
      folderName + '/combined.csv',
    );
    console.log(`combined.csv created`);
  }

  private __getValidationsByTypes(results: EmailValidationResponseType[]) {
    let valid_email_count = 0;
    let catch_all_count = 0;
    let spam_trap_count = 0;
    let invalid_email_count = 0;
    let do_not_mail_count = 0;
    let unknown_count = 0;


    results.forEach((email: EmailValidationResponseType) => {
      if (email.email_status === EmailStatus.VALID) {
        valid_email_count++;
      } else if (email.email_status === EmailStatus.CATCH_ALL) {
        catch_all_count++;
      } else if (email.email_status === EmailStatus.SPAMTRAP) {
        spam_trap_count++;
      } else if (
        email.email_status === EmailStatus.INVALID ||
        email.email_status === EmailStatus.INVALID_DOMAIN
      ) {
        invalid_email_count++;
      } else if (
        // Count 'IP_BLOCKED', 'UNVERIFIABLE_EMAIL' & 'SMTP_TIMEOUT' as unknown to report to user properly.
        (email.email_status === EmailStatus.UNKNOWN && email.email_sub_status === EmailReason.UNVERIFIABLE_EMAIL) ||
        (email.email_status === EmailStatus.UNKNOWN && email.email_sub_status === EmailReason.SMTP_TIMEOUT) ||
        (email.email_status === EmailStatus.SERVICE_UNAVAILABLE && email.email_sub_status === EmailReason.IP_BLOCKED)
      ) {
        unknown_count++;
      } else if (email.email_status === EmailStatus.DO_NOT_MAIL) {
        do_not_mail_count++;
      }
    });

    return {
      valid_email_count,
      invalid_email_count,
      unknown_count,
      catch_all_count,
      do_not_mail_count,
      spam_trap_count,
    };
  }

  private async __bulkValidate(bulkFile: BulkFile, user: User): Promise<EmailValidationResponseType[]> {
    if (!bulkFile.file_path) {
      throw new Error('No file path provided');
    }

    let batchSize = 10;
    let delayBetweenBatches = 10 * 1000;
    let limiter = new Bottleneck({
      maxConcurrent: 2, // Adjust based on your testing
      minTime: 300, // 300ms delay between requests (adjustable)
    });
    const results: EmailValidationResponseType[] = [];

    try {
      const bulkFileEmails: BulkFileEmail[] = await this.bulkFileEmailsService.findBulkFileEmails(bulkFile.id);
      const hotmailEmails: BulkFileEmail[] = [];
      const hotmailMxRecord = 'hotmail-com.olc.protection.outlook.com';
      const liveEmails: BulkFileEmail[] = [];
      const liveMxRecord = 'live-com.olc.protection.outlook.com';
      const msnEmails: BulkFileEmail[] = [];
      const msnMxRecord = 'msn-com.olc.protection.outlook.com';
      const outlookEmails: BulkFileEmail[] = [];
      const outlookMxRecord = 'outlook-com.olc.protection.outlook.com';
      const outlookBusinessDomainEmails: BulkFileEmail[] = [];
      const nonOutlookEmails: BulkFileEmail[] = [];
      for (const bulkFileEmail of bulkFileEmails) {
        const [account, domain] = bulkFileEmail.email_address.split('@');
        console.log({ domain });
        let mxRecords = [];
        try {
          mxRecords = await this.domainService.checkDomainMxRecords(domain, null);
        } catch (e) {
          results.push({
            email_address: bulkFileEmail.email_address,
            ...e,
          });
          continue;
        }
        // If MX record includes 'outlook.com' it means mail server is outlook.
        // We split Outlook emails into few categories process them differently.
        if (mxRecords[0].exchange.includes(MicrosoftDomains.OUTLOOK)) {
          if (domain === MicrosoftDomains.HOTMAIL) {
            hotmailEmails.push(bulkFileEmail);
          } else if (domain === MicrosoftDomains.OUTLOOK) {
            outlookEmails.push(bulkFileEmail);
          } else if (domain === MicrosoftDomains.LIVE) {
            liveEmails.push(bulkFileEmail);
          } else if (domain === MicrosoftDomains.MSN) {
            msnEmails.push(bulkFileEmail);
          } else {
            outlookBusinessDomainEmails.push(bulkFileEmail);
          }
        } else {
          nonOutlookEmails.push(bulkFileEmail);
        }
      }

     /* const hotmailEmailResults: EmailValidationResponseType[] = await this.__processOutlookBatch(
        batchSize, hotmailEmails, delayBetweenBatches, hotmailMxRecord, user, bulkFile,
      );
      if (hotmailEmailResults.length) {
        results.push(...hotmailEmailResults);
      }

      const liveEmailResults: EmailValidationResponseType[] = await this.__processOutlookBatch(
        batchSize, liveEmails, delayBetweenBatches, liveMxRecord, user, bulkFile,
      );
      if (liveEmailResults.length) {
        results.push(...liveEmailResults);
      }

      const outlookEmailResults: EmailValidationResponseType[] = await this.__processOutlookBatch(
        batchSize, outlookEmails, delayBetweenBatches, outlookMxRecord, user, bulkFile,
      );
      if (outlookEmailResults.length) {
        results.push(...outlookEmailResults);
      }

      const msnEmailResults: EmailValidationResponseType[] = await this.__processOutlookBatch(
        batchSize, msnEmails, delayBetweenBatches, msnMxRecord, user, bulkFile,
      );
      if (msnEmailResults.length) {
        results.push(...msnEmailResults);
      }*/

      // Split emails into batches
      const nonOutlookEmailBatches = this.__createBatchOfSize(batchSize, nonOutlookEmails);
      // For Outlook, each batch should have only 1 email.
/*
      batchSize = 1;
      const outlookBusinessEmailBatches = this.__createBatchOfSize(batchSize, outlookBusinessDomainEmails);
*/

      // Process each batch sequentially
      for (const batch of nonOutlookEmailBatches) {
        const result: EmailValidationResponseType[] = await this.__processBatchValidation(batch, limiter, user, bulkFile);
        if (result.length) {
          results.push(...result);
        }
        await this.__delay(delayBetweenBatches);
      }
      // For Outlook mail server we slow the limiter to only 1 per concurrency
      // and delay between batches is 2 sec as batch size is 1 email
      /*limiter = new Bottleneck({
        maxConcurrent: 1,
      });
      delayBetweenBatches = 2 * 1000;
      for (const batch of outlookBusinessEmailBatches) {
        const result: EmailValidationResponseType[] = await this.__processBatchValidation(batch, limiter, user, bulkFile);
        if (result.length) {
          results.push(...result);
        }
        await this.__delay(delayBetweenBatches);
      }*/

      console.log('✅ All batches processed.');
      return results;
    } catch (err) {
      console.error('❌ Error during bulk validation:', err);
      throw err; // Re-throw to let the caller handle it
    }
  }

  private async __processOutlookBatch(
    batchSize: number,
    emails: BulkFileEmail[],
    delayBetweenBatches: number,
    mxRecordHost: string,
    user: User,
    bulkFile: BulkFile,
  ): Promise<EmailValidationResponseType[]> {
    const results: EmailValidationResponseType[] = [];
    if (!emails.length) {
      return results;
    }
    // Split emails into batches
    const emailBatches = this.__createBatchOfSize(batchSize, emails);
    // Process each batch sequentially
    for (const batch of emailBatches) {
      const hotmailResponse: EmailValidationResponseType[] = await this.domainService.bulkOutlookEmailVerification(batch, mxRecordHost, user, bulkFile.id);
      if (hotmailResponse.length) {
        results.push(...hotmailResponse);
      }
      await this.__delay(delayBetweenBatches);
    }
    return results;
  }

  private async __delay(delay: number) {
    await new Promise(resolve => setTimeout(resolve, delay));
  }

  private __createBatchOfSize(size: number, emails: BulkFileEmail[]) {
    const batch = [];
    for (let i = 0; i < emails.length; i += size) {
      batch.push(emails.slice(i, i + size));
    }

    return batch;
  }

  private async __processBatchValidation(batch: BulkFileEmail[], limiter, user, bulkFile): Promise<EmailValidationResponseType[]> {
    const results: EmailValidationResponseType[] = [];
    const batchPromises = batch.map((bulkFileEmail) =>
      limiter.schedule(async () => {
        try {
          console.log(`Validation started: ${bulkFileEmail.email_address}`);
          const validationResponse: EmailValidationResponseType = await this.domainService.smtpValidation(
            bulkFileEmail.email_address,
            user,
            bulkFile.id,
          );
          console.log(`Validation complete: ${validationResponse.email_address}`);
          return validationResponse;
        } catch (error) {
          console.error(`Error validating ${bulkFileEmail.email_address}:`, error);
          return null; // Capture the error instead of failing the batch
        }
      }),
    );

    // Wait for the batch to complete
    const batchResults = await Promise.allSettled(batchPromises);
    results.push(
      ...batchResults
        .filter(result => result.status === 'fulfilled' && result.value !== null)
        .map(result => (result as PromiseFulfilledResult<any>).value),
    );

    return results;
  }


  private async __oldBulkValidate(bulkFile: BulkFile, user: User): Promise<any[]> {
    if (!bulkFile.file_path) {
      throw new Error('No file path provided');
    }
    let csvHeaders = [];
    // Bottleneck for rate limiting (CommonJS compatible)
    const limiter = new Bottleneck({
      maxConcurrent: 2, // Adjust based on your testing
      minTime: 300, // 300ms delay between requests (adjustable)
    });

    try {
      const bulkFileEmails: BulkFileEmail[] = await this.bulkFileEmailsService.findBulkFileEmails(bulkFile.id);
      // const records = await this.bulkFilesService.readCsvFile(bulkFile.file_path);
      // console.log({ records });

      // const results = [];
      // for (const record of records) {
      //   console.log(`Validation started: ${record.Email}`);
      //   const validationResponse: EmailValidationResponseType = await this.domainService.smtpValidation(
      //     record.Email,
      //     user,
      //     bulkFile.id,
      //   );
      //   // console.log(`Validation done: ${validationResponse.email_address}`);
      //   // Add emails to GreyList check
      //   if (
      //     validationResponse.email_sub_status === EmailReason.GREY_LISTED
      //   ) {
      //     await this.queueService.addGreyListEmailToQueue(validationResponse);
      //   }
      //   const res = {
      //     ...record,
      //     ...validationResponse,
      //   };
      //   results.push(res);
      // }
      // return results;


      // Validate emails in parallel
      const validationPromises: Promise<any>[] = bulkFileEmails.map((bulkFileEmail) => limiter.schedule(async () => {
          console.log(`Validation started: ${bulkFileEmail.email_address}`);
          const validationResponse: EmailValidationResponseType = await this.domainService.smtpValidation(
            bulkFileEmail.email_address,
            user,
            bulkFile.id,
          );
          console.log(`Complete ${validationResponse.email_address}`);
          return validationResponse;
        }),
      );
      // Wait for all validations to complete
      const results = await Promise.allSettled(validationPromises);
      return results
        .filter(result => result.status === 'fulfilled')
        .map(result => (result as PromiseFulfilledResult<any>).value);
    } catch (err) {
      console.error('Error during bulk validation:', err);
      throw err;
    }
  }

  private async __readSCsvAndMergeValidationResults(csvPath: string) {
    try {
      const records = await this.bulkFilesService.readCsvFile(csvPath);

      for (let record of records) {
        const processedEmail: ProcessedEmail = await this.domainService.getProcessedEmail(record.Email);
        if (!processedEmail) {
          continue;
        }
        // Delete these property so these are not included in the final response.
        delete processedEmail.id;
        delete processedEmail.user_id;
        delete processedEmail.bulk_file_id;
        delete processedEmail.created_at;
        delete processedEmail.retry;
        record = Object.assign(record, processedEmail); // Merges source into target (modifies target)
      }

      return records;
    } catch (err) {
      console.error('Error during bulk validation:', err);
      throw err;
    }
  }

}
