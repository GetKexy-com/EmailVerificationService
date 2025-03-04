import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';

import { BulkFilesController } from '@/bulk-files/bulk-files.controller';
import { BulkFilesService } from '@/bulk-files/bulk-files.service';
import { BulkFile } from '@/bulk-files/entities/bulk-file.entity';
import { QueueModule } from '@/queue/queue.module';
import { ConfigModule } from '@nestjs/config';

@Module({
  imports: [
    TypeOrmModule.forFeature([BulkFile]),
    QueueModule,
    ConfigModule
  ],
  controllers: [BulkFilesController],
  providers: [BulkFilesService],
  exports: [BulkFilesService],
})
export class BulkFilesModule {
}
