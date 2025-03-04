import { PartialType } from '@nestjs/mapped-types';

import { CreateBulkFileDto } from '@/bulk-files/dto/create-bulk-file.dto';

export class UpdateBulkFileDto extends PartialType(CreateBulkFileDto) {}
