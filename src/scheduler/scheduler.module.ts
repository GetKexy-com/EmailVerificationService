import { Module } from '@nestjs/common';
import { ScheduleModule } from '@nestjs/schedule';

import { BulkFilesModule } from '@/bulk-files/bulk-files.module';
import { DomainsModule } from '@/domains/domains.module';
import { SchedulerService } from '@/scheduler/scheduler.service';
import { MailerModule } from '@/mailer/mailer.module';
import { WinstonLoggerModule } from '@/logger/winston-logger.module';
import { UsersModule } from '@/users/users.module';
import { TimeModule } from '@/time/time.module';
import { QueueModule } from '@/queue/queue.module';
import { BulkFileEmailsModule } from '@/bulk-file-emails/bulk-file-emails.module';

@Module({
  providers: [SchedulerService],
  imports: [
    ScheduleModule.forRoot(),
    BulkFilesModule,
    BulkFileEmailsModule,
    DomainsModule,
    UsersModule,
    TimeModule,
    QueueModule,
    WinstonLoggerModule,
  ],
})
export class SchedulerModule {
}
