import { IsDate, IsNotEmpty, IsNumber, IsOptional, IsString } from 'class-validator';

import { BulkFileStatus } from '@/bulk-files/entities/bulk-file.entity';

export class CreateBulkFileDto {
  @IsString()
  @IsNotEmpty()
  readonly file_original_name: string;

  @IsString()
  @IsNotEmpty()
  readonly file_path: string;

  @IsNumber()
  @IsNotEmpty()
  readonly user_id: number;

  @IsNumber()
  @IsNotEmpty()
  total_email_count: number;

  @IsString()
  @IsOptional()
  file_status: BulkFileStatus;

  @IsString()
  @IsOptional()
  validation_file_path: string;

  @IsString()
  @IsOptional()
  do_not_mail_count: number;

  @IsString()
  @IsOptional()
  catch_all_count: number;

  @IsString()
  @IsOptional()
  unknown_count: number;

  @IsString()
  @IsOptional()
  spam_trap_count: number;

  @IsString()
  @IsOptional()
  invalid_email_count: number;

  @IsString()
  @IsOptional()
  valid_email_count: number;

  @IsDate()
  @IsOptional()
  updated_at?: Date;
}
