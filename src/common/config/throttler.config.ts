import { ThrottlerStorageRedisService } from '@nest-lab/throttler-storage-redis';
import { ExecutionContext, Injectable } from '@nestjs/common';
import {
  hours,
  minutes,
  ThrottlerModuleOptions,
  ThrottlerOptionsFactory,
} from '@nestjs/throttler';
import Redis from 'ioredis';
import { config } from 'dotenv';
import * as process from 'process';

config(); // Load .env file into process.env

@Injectable()
export class ThrottlerConfigService implements ThrottlerOptionsFactory {
  constructor() {
  }

  createThrottlerOptions(): ThrottlerModuleOptions {
    return {
      throttlers: [
        {
          limit: 50000,
          ttl: hours(1),
          blockDuration: minutes(5),
        },
      ],
      errorMessage: 'Rate limit exceeded. Please try again later.',
      storage: new ThrottlerStorageRedisService(
        new Redis({
          host: process.env.REDIS_DB_HOST,
          port: parseInt(process.env.REDIS_DB_PORT),
        }),
      ),
      getTracker: (req: Record<string, any>, context: ExecutionContext) => {
        return req.headers['api_key'];
      },
      generateKey: (
        context: ExecutionContext,
        trackerString: string,
        throttlerName: string,
      ) => {
        return `${trackerString}-${throttlerName}`;
      },
    };
  }
}
