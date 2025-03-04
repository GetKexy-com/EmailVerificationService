import { Module } from '@nestjs/common';
import { SmtpConnectionService } from './smtp-connection.service';
import { WinstonLoggerModule } from '@/logger/winston-logger.module';

@Module({
  imports: [WinstonLoggerModule],
  providers: [SmtpConnectionService],
  exports: [SmtpConnectionService],
})
export class SmtpConnectionModule {
}
