import { HttpException, HttpStatus, Injectable } from '@nestjs/common';
import { BulkFile, BulkFileStatus } from '@/bulk-files/entities/bulk-file.entity';
import * as fs from 'fs';
import * as csv from 'csv-parse';
import { BulkFileEmail } from '@/bulk-file-emails/entities/bulk-file-email.entity';
import { DataSource } from 'typeorm';
import { Domain } from '@/domains/entities/domain.entity';

@Injectable()
export class BulkFileEmailsService {

  constructor(
    private dataSource: DataSource,
  ) {
  }

  async findBulkFileEmails(bulkFileId: number): Promise<BulkFileEmail[]> {
    return BulkFileEmail.findBy({ bulk_file_id: bulkFileId });
  }

  async saveBulkFileEmails(bulkFile: BulkFile) {
    const records = await this.readCsvFile(bulkFile.file_path);
    const bulkFileEmails = [];
    for (const record of records) {
      bulkFileEmails.push({
        bulk_file_id: bulkFile.id,
        email_address: record.Email,
        user_id: bulkFile.user_id,
      });
    }

    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();
    try {
      await queryRunner.manager.save(BulkFileEmail, bulkFileEmails);
      await queryRunner.commitTransaction();
    } catch (err) {
      // since we have errors lets rollback the changes we made
      await queryRunner.rollbackTransaction();
      throw new HttpException(err.message, HttpStatus.BAD_REQUEST);
    } finally {
      // you need to release a queryRunner which was manually instantiated
      await queryRunner.release();
    }

    await BulkFile.update(bulkFile.id, { ...bulkFile, file_status: BulkFileStatus.PENDING });

  }


  async readCsvFile(csvPath: string): Promise<any> {
    try {
      // Read the CSV file
      const data = await fs.promises.readFile(csvPath, 'utf8');
      // Parse the CSV content
      return await new Promise<any[]>((resolve, reject) => {
        csv.parse(
          data,
          {
            columns: true, // Convert rows to objects using the first row as keys
            skip_empty_lines: true, // Ignore empty lines
            trim: true, // Trim spaces from values
          },
          (err, records) => {
            if (err) {
              reject(err);
            } else {
              resolve(records);
            }
          },
        );
      });
    } catch (err) {
      console.error('Error during bulk validation:', err);
      throw err;
    }

  }

}
