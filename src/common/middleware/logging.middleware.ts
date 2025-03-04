import { Injectable, NestMiddleware } from '@nestjs/common';
import { FastifyReply, FastifyRequest } from 'fastify';

@Injectable()
export class LoggingMiddleware implements NestMiddleware {
  // use(req: any, res: any, next: () => void) {
  //   next();
  // }

  use(req: FastifyRequest['raw'], res: FastifyReply['raw'], next: () => void) {
    console.log('Request...');
    next();
  }
}
