import { Module } from '@nestjs/common';
import { BulkFileEmailsService } from './bulk-file-emails.service';
import { TypeOrmModule } from '@nestjs/typeorm';
import { BulkFileEmail } from '@/bulk-file-emails/entities/bulk-file-email.entity';

@Module({
  imports: [
    TypeOrmModule.forFeature([BulkFileEmail]),
  ],
  providers: [BulkFileEmailsService],
  exports: [BulkFileEmailsService],
})
export class BulkFileEmailsModule {
}
