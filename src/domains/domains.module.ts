import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';

import { DisposableDomainsModule } from '@/disposable-domains/disposable-domains.module';
import { EmailRolesModule } from '@/email-roles/email-roles.module';
import { DomainsController } from '@/domains/domains.controller';
import { Domain } from '@/domains/entities/domain.entity';
import { ErrorDomain } from '@/domains/entities/error_domain.entity';
import { ProcessedEmail } from '@/domains/entities/processed_email.entity';
import { DomainService } from '@/domains/services/domain.service';
import { WinstonLoggerModule } from '@/logger/winston-logger.module';
import { MailerModule } from '@/mailer/mailer.module';
import { ConfigModule } from '@nestjs/config';
import { SmtpConnectionModule } from '@/smtp-connection/smtp-connection.module';
import { UsersModule } from '@/users/users.module';
import { TimeModule } from '@/time/time.module';

@Module({
  imports: [
    TypeOrmModule.forFeature([Domain, ErrorDomain, ProcessedEmail]),
    DisposableDomainsModule,
    EmailRolesModule,
    MailerModule,
    ConfigModule,
    SmtpConnectionModule,
    WinstonLoggerModule,
    UsersModule,
    TimeModule,
  ],
  controllers: [DomainsController],
  providers: [DomainService],
  exports: [DomainService],
})
export class DomainsModule {
}
