import { PartialType } from '@nestjs/mapped-types';
import { CreateBulkFileEmailDto } from './create-bulk-file-email.dto';

export class UpdateBulkFileEmailDto extends PartialType(CreateBulkFileEmailDto) {}
