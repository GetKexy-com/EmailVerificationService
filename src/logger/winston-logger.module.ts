import { Module } from '@nestjs/common';
import { WinstonLoggerService } from '@/logger/winston-logger.service';

@Module({
  exports: [WinstonLoggerService],
  providers: [WinstonLoggerService]
})
export class WinstonLoggerModule {
}
