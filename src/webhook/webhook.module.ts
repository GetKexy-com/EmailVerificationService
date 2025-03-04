import { Module } from '@nestjs/common';
import { WebhookService } from './webhook.service';
import { WebhookController } from './webhook.controller';
import { WinstonLoggerModule } from '@/logger/winston-logger.module';
import { DomainsModule } from '@/domains/domains.module';
import { SmtpConnectionModule } from '@/smtp-connection/smtp-connection.module';

@Module({
  imports: [WinstonLoggerModule, DomainsModule, SmtpConnectionModule],
  controllers: [WebhookController],
  providers: [WebhookService],
})
export class WebhookModule {
}
