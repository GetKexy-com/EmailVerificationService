import { MiddlewareConsumer, Module, NestModule } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { APP_GUARD } from '@nestjs/core';

import { DisposableDomainsModule } from '@/disposable-domains/disposable-domains.module';
import { DomainsModule } from '@/domains/domains.module';
import { EmailRolesModule } from '@/email-roles/email-roles.module';
import { ApiKeyGuard } from '@/common/guards/api-key.guard';
import { LoggingMiddleware } from '@/common/middleware/logging.middleware';

@Module({
  imports: [
    ConfigModule,
    DomainsModule,
    DisposableDomainsModule,
    EmailRolesModule,
  ],
  providers: [{ provide: APP_GUARD, useClass: ApiKeyGuard }],
})
export class CommonModule implements NestModule {
  configure(consumer: MiddlewareConsumer): any {
    consumer.apply(LoggingMiddleware).forRoutes('*');
  }
}
