import * as process from 'node:process';

import multipart from '@fastify/multipart';
import { ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import {
  FastifyAdapter,
  NestFastifyApplication,
} from '@nestjs/platform-fastify';

import { AppModule } from './app.module';
import { ResponseSuccessInterceptor } from './common/interceptors/response-success.interceptor';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter(),
  );
  // Register the multipart plugin
  await app.register(multipart);

  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      transform: true,
      forbidNonWhitelisted: true,
      transformOptions: {
        enableImplicitConversion: true,
      },
    }),
  );
  app.useGlobalInterceptors(new ResponseSuccessInterceptor());

  // process.on('unhandledRejection', (reason, promise) => {
  //   console.error('Unhandled Promise Rejection:', reason);
  // });

  await app.listen(process.env.PORT ?? 3000, '0.0.0.0');
}

bootstrap().then();
