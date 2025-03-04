import { Module } from '@nestjs/common';
import { QueueService } from './queue.service';
import { BullModule } from '@nestjs/bull';
import * as process from 'node:process';
import { config } from 'dotenv';

import { QUEUE } from '@/common/utility/constant';
import { QueueProcessor } from '@/queue/queue.processor';
import { MailerModule } from '@/mailer/mailer.module';
import { DomainsModule } from '@/domains/domains.module';
import { WinstonLoggerModule } from '@/logger/winston-logger.module';
import { SmtpConnectionModule } from '@/smtp-connection/smtp-connection.module';
import { BulkFileEmailsModule } from '@/bulk-file-emails/bulk-file-emails.module';

config(); // Load .env file into process.env

@Module({
  imports: [
    BullModule.forRoot({
      redis: {
        host: process.env.REDIS_DB_HOST,
        port: parseInt(process.env.REDIS_DB_PORT),
      },
      prefix: 'queue_service:', // Use a unique prefix
    }),
    BullModule.registerQueue({
      name: QUEUE,
    }),
    MailerModule,
    DomainsModule,
    BulkFileEmailsModule,
    SmtpConnectionModule,
    WinstonLoggerModule,
  ],
  providers: [QueueService, QueueProcessor],
  exports: [QueueService, BullModule],
})
export class QueueModule {
}
