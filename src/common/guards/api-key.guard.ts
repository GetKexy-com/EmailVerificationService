import {
  CanActivate,
  ExecutionContext,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Reflector } from '@nestjs/core';
import { FastifyRequest } from 'fastify';
import { Observable } from 'rxjs';

import { IS_PUBLIC_KEY } from '@/common/decorators/public.decorator';

@Injectable()
export class ApiKeyGuard implements CanActivate {
  constructor(
    private readonly reflector: Reflector,
    private readonly configService: ConfigService,
  ) {}

  canActivate(
    context: ExecutionContext,
  ): boolean | Promise<boolean> | Observable<boolean> {
    // Check if the API route has a metadata 'Public'
    const isPublic = this.reflector.get(IS_PUBLIC_KEY, context.getHandler());
    if (isPublic) return true;

    const request = context.switchToHttp().getRequest<FastifyRequest>();
    const authHeader = request.headers.api_key;

    // if (authHeader !== this.configService.get('API_KEY')) {
    //   throw new UnauthorizedException('Invalid API key');
    // }

    return true;
  }
}
