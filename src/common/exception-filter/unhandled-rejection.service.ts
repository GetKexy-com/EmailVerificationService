import { Injectable, OnApplicationBootstrap } from '@nestjs/common';
import { WinstonLoggerService } from '@/logger/winston-logger.service';

@Injectable()
export class UnhandledRejection implements OnApplicationBootstrap {
  constructor(private readonly logger: WinstonLoggerService) {
  }

  onApplicationBootstrap() {
    process.on('unhandledRejection', (reason: any, promise) => {
      this.logger.error('Unhandled Promise Rejection:', reason);
    });

    process.on('uncaughtException', (error: Error) => {
      this.logger.error('Uncaught Exception:', error.message);
      process.exit(1); // Optional: force restart the process
    });
  }
}
