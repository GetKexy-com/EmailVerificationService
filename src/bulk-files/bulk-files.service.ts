import * as fs from 'node:fs';

import { HttpException, HttpStatus, Injectable } from '@nestjs/common';
import { plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';
import * as csv from 'csv-parse';
import { createObjectCsvWriter } from 'csv-writer';
import * as path from 'path';

import { CsvUploadDto } from '@/common/dto/csv-upload.dto';
import { CreateBulkFileDto } from '@/bulk-files/dto/create-bulk-file.dto';
import { UpdateBulkFileDto } from '@/bulk-files/dto/update-bulk-file.dto';
import { BulkFile, BulkFileStatus } from '@/bulk-files/entities/bulk-file.entity';
import * as process from 'node:process';
import * as constants from 'node:constants';
import { DEV } from '@/common/utility/constant';

@Injectable()
export class BulkFilesService {
  async getPendingBulkFile() {
    return await BulkFile.find({
      where: { file_status: BulkFileStatus.PENDING },
      order: { id: 'ASC' },
      take: 1,
    });
  }

  async getBulkFile(fileId: number) {
    return await BulkFile.findOne({ where: { id: fileId } });
  }

  async getGreyListCheckBulkFile() {
    return await BulkFile.find({
      where: { file_status: BulkFileStatus.GREY_LIST_CHECK_DONE },
      order: { id: 'ASC' },
      take: 1,
    });
  }

  async getGreyListReadyBulkFile() {
    return await BulkFile.find({
      where: { file_status: BulkFileStatus.READY_FOR_GREY_LIST },
      order: { id: 'ASC' },
      take: 1,
    });
  }

  async saveBulkFile(createBulkFileDto: CreateBulkFileDto) {
    const bulkFile: BulkFile = BulkFile.create({ ...createBulkFileDto });
    return bulkFile.save();
  }

  async updateBulkFile(id: number, updateBulkFileDto: UpdateBulkFileDto) {
    try {
      const bulkFile: BulkFile = await BulkFile.findOneBy({ id });
      const updateData = { ...bulkFile, ...updateBulkFileDto };

      await BulkFile.update(id, updateData);
      return updateData;
    } catch (e) {
      throw new HttpException(e, HttpStatus.BAD_REQUEST);
    }
  }

  async generateCsv(data: any[], filename: string): Promise<string> {
    const uploadPath: string = process.env.NODE_ENV === DEV ? '/uploads' : '../uploads';
    const csvSavePath = path.join(process.cwd(), uploadPath, 'csv', 'validated', filename);
    console.log({ csvSavePath });
    // Ensure the directory exists
    if (!fs.existsSync(path.dirname(csvSavePath))) {
      fs.mkdirSync(path.dirname(csvSavePath), { recursive: true });
    }
    const LW_keys = [
      'email_status',
      'account',
      'domain',
      'email_address',
      'email_sub_status',
      'domain_age_days',
      'free_email',
    ];
    const csvHeaders = [];
    Object.keys(data[0]).forEach(key => {
      if (!LW_keys.includes(key)) {
        csvHeaders.push({
          id: key,
          title: key,
        });
      }
    });

    const csvWriter = createObjectCsvWriter({
      path: csvSavePath,
      header: [
        ...csvHeaders,
        { id: 'account', title: 'LW Account' },
        { id: 'domain', title: 'LW Domain' },
        { id: 'email_status', title: 'LW Email Status' },
        { id: 'email_sub_status', title: 'LW Email Sub Status' },
        { id: 'domain_age_days', title: 'LW Domain Age Days' },
        { id: 'free_email', title: 'LW Free Email' },
      ],
    });

    await csvWriter.writeRecords(data);

    return csvSavePath;
  }

  async validateCsvData(file): Promise<any> {
    const csvContent = file;
    const parsedData: any = await new Promise((resolve, reject) => {
      csv.parse(
        csvContent,
        {
          columns: true,
          relax_quotes: true,
          skip_empty_lines: true,
          cast: true,
        },
        (err, records) => {
          if (err) {
            reject(err);
            return { error: true, message: 'Unable to parse file' };
          }
          resolve(records);
        },
      );
    });
    const errors: string[] = [];
    if (!parsedData.length) {
      errors.push('Empty File Provided');
      return {
        error: true,
        message: 'File Validation Failed',
        errorsArray: errors,
      };
    }
    //validate All Rows
    let rowCount = 0;
    for await (const [index, rowData] of parsedData.entries()) {
      const validationErrors = await this.validateFileRow(rowData);
      if (validationErrors.length) {
        return {
          error: true,
          message: `File Rows Validation Failed at row: ${index + 1} - ${validationErrors} ${JSON.stringify(rowData)}`,
        };
      }
      rowCount++;
    }
    return { error: false, total_emails: rowCount };
  }

  async validateFileRow(rowData) {
    const errors: string[] = [];
    const csvDto = plainToInstance(CsvUploadDto, rowData);
    const validationErrors = await validate(csvDto);
    if (validationErrors.length > 0) {
      validationErrors.forEach((error) => {
        const { property, constraints } = error;
        const errorMessage = `${property}: ${Object.values(constraints).join(', ')}`;
        errors.push(errorMessage);
      });
    }
    return errors;
  }

  async __getAllFilesInFolder(folderPath: string): Promise<string[]> {
    return new Promise((resolve, reject) => {
      fs.readdir(folderPath, (err, files) => {
        if (err) {
          reject(`Unable to scan directory: ${err}`);
        } else {
          // Filter out directories and return only files
          const filePaths = files
            .map((file) => path.join(folderPath, file))
            .filter((filePath) => fs.statSync(filePath).isFile());
          resolve(filePaths);
        }
      });
    });
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
