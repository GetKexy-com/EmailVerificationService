import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { APP_FILTER, APP_GUARD } from '@nestjs/core';
import { ThrottlerGuard, ThrottlerModule } from '@nestjs/throttler';
import { TypeOrmModule } from '@nestjs/typeorm';

import { AppController } from './app.controller';
import { AppService } from './app.service';
import { AuthModule } from './auth/auth.module';
import { BulkFilesModule } from './bulk-files/bulk-files.module';
import { CommonModule } from './common/common.module';
import { ThrottlerConfigService } from './common/config/throttler.config';
import { SchedulerModule } from './scheduler/scheduler.module';
import { UsersModule } from './users/users.module';
import { MailerService } from './mailer/mailer.service';
import { MailerModule } from './mailer/mailer.module';
import { WinstonLoggerService } from './logger/winston-logger.service';
import { WinstonLoggerModule } from '@/logger/winston-logger.module';
import { GlobalExceptionFilter } from '@/common/exception-filter/global-exception.filter';
import { TimeService } from './time/time.service';
import { TimeModule } from './time/time.module';
import { QueueModule } from './queue/queue.module';
import { WebhookModule } from './webhook/webhook.module';
import { EventEmitterModule } from '@nestjs/event-emitter';
import { SmtpConnectionModule } from './smtp-connection/smtp-connection.module';
import { UnhandledRejection } from '@/common/exception-filter/unhandled-rejection.service';
import { BulkFileEmailsModule } from './bulk-file-emails/bulk-file-emails.module';

@Module({
  imports: [
    ConfigModule.forRoot(),
    ThrottlerModule.forRootAsync({
      useClass: ThrottlerConfigService,
    }),
    TypeOrmModule.forRoot({
      type: 'postgres',
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
