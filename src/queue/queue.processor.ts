import { Process, Processor } from '@nestjs/bull';
import { Job } from 'bull';
import {
  PROCESS_EMAIL_SEND_QUEUE,
  PROCESS_GREY_LIST_QUEUE,
  QUEUE,
  PROCESS_BULK_FILE_QUEUE,
} from '@/common/utility/constant';
import { QueueService } from '@/queue/queue.service';
import { EmailValidationResponseType } from '@/common/utility/email-status-type';
import { BulkFile } from '@/bulk-files/entities/bulk-file.entity';

@Processor(QUEUE)
export class QueueProcessor {
  constructor(
    private readonly queueService: QueueService,
  ) {
  }

  @Process(PROCESS_GREY_LIST_QUEUE)
  async handleQueueTask(job: Job<EmailValidationResponseType[]>) {
    console.log('Processing Queue:', job.data);
    return this.queueService.runGrayListCheck(job.data);
  }

  @Process(PROCESS_EMAIL_SEND_QUEUE)
  async handleSendEmail(job: Job<{ to: string; subject: string; template: string; context: any }>) {
    console.log('Email Sending...');
    const emailResponse = await this.queueService.processEmailQueue(job.data);
    console.log(emailResponse);
    return emailResponse;
  }

  @Process(PROCESS_BULK_FILE_QUEUE)
  async handleBulkFileQueueTask(job: Job<BulkFile>) {
    return this.queueService.saveBulkFileEmails(job.data);
  }

}
