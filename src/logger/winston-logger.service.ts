import { Injectable, LoggerService } from '@nestjs/common';
import * as winston from 'winston';
import * as path from 'path';
import * as fs from 'fs';
import * as DailyRotateFile from 'winston-daily-rotate-file';

@Injectable()
export class WinstonLoggerService implements LoggerService {
  private logger: winston.Logger;

  constructor() {
    // Define the log directory
    const logDir = path.join(process.cwd(), 'logs');

    // Create the logs directory if it doesn't exist
    if (!fs.existsSync(logDir)) {
      fs.mkdirSync(logDir);
    }

    // Create the logger
    this.logger = winston.createLogger({
      level: 'info',
      format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.json(),
      ),
      transports: [
        new winston.transports.Console(),
        new DailyRotateFile({
          level: 'error',
          filename: path.join(logDir, 'error-%DATE%.log'),
          datePattern: 'YYYY-MM-DD',
          zippedArchive: true,
          maxSize: '20m',
          maxFiles: '7d',
        }),
        new DailyRotateFile({
          level: 'warn',
          filename: path.join(logDir, 'combined-%DATE%.log'),
          datePattern: 'YYYY-MM-DD',
          zippedArchive: true,
          maxSize: '20m',
          maxFiles: '7d',
        }),
      ],
    });
  }

  log(message: string) {
    this.logger.info(message);
  }

  error(message: string, trace: string) {
    this.logger.error(message, { trace });
  }

  warn(message: string) {
    this.logger.warn(message);
  }

  debug(message: string) {
    this.logger.debug(message);
  }

  verbose(message: string) {
    this.logger.verbose(message);
  }
}
