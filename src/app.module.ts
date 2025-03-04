import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { APP_FILTER, APP_GUARD } from '@nestjs/core';
import { EventEmitterModule } from '@nestjs/event-emitter';
import { ThrottlerGuard, ThrottlerModule } from '@nestjs/throttler';
import { TypeOrmModule } from '@nestjs/typeorm';

import { GlobalExceptionFilter } from '@/common/exception-filter/global-exception.filter';
import { UnhandledRejection } from '@/common/exception-filter/unhandled-rejection.service';
import { WinstonLoggerModule } from '@/logger/winston-logger.module';

import { AppController } from './app.controller';
import { AppService } from './app.service';
import { AuthModule } from './auth/auth.module';
import { BulkFileEmailsModule } from './bulk-file-emails/bulk-file-emails.module';
import { BulkFilesModule } from './bulk-files/bulk-files.module';
import { CommonModule } from './common/common.module';
import { ThrottlerConfigService } from './common/config/throttler.config';
import { WinstonLoggerService } from './logger/winston-logger.service';
import { MailerModule } from './mailer/mailer.module';
import { MailerService } from './mailer/mailer.service';
import { QueueModule } from './queue/queue.module';
import { SchedulerModule } from './scheduler/scheduler.module';
import { SmtpConnectionModule } from './smtp-connection/smtp-connection.module';
import { TimeModule } from './time/time.module';
import { TimeService } from './time/time.service';
import { UsersModule } from './users/users.module';
import { WebhookModule } from './webhook/webhook.module';

@Module({
  imports: [
    ConfigModule.forRoot(),
    ThrottlerModule.forRootAsync({
      useClass: ThrottlerConfigService,
    }),
    TypeOrmModule.forRoot({
      type: `postgres`,
      host: process.env.DATABASE_HOST,
      port: parseInt(process.env.DATABASE_PORT, 5432),
      username: process.env.DATABASE_USER,
      password: process.env.DATABASE_PASSWORD,
      database: process.env.DATABASE_NAME,
      autoLoadEntities: true,
      synchronize: true,
    }),
    EventEmitterModule.forRoot(),
    CommonModule,
    SchedulerModule,
    BulkFilesModule,
    AuthModule,
    UsersModule,
    MailerModule,
    WinstonLoggerModule,
    TimeModule,
    QueueModule,
    WebhookModule,
    SmtpConnectionModule,
    BulkFileEmailsModule,
  ],
  controllers: [AppController],
  providers: [
    AppService,
    {
      provide: APP_GUARD,
      useClass: ThrottlerGuard,
    },
    MailerService,
    WinstonLoggerService,
    UnhandledRejection,
    {
      provide: APP_FILTER,
      useClass: GlobalExceptionFilter,
    },
    TimeService,
  ],
})
export class AppModule {
}
