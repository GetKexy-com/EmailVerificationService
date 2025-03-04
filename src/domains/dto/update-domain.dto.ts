import { PartialType } from '@nestjs/mapped-types';

import { CreateDomainDto } from '@/domains/dto/create-domain.dto';

export class UpdateDomainDto extends PartialType(CreateDomainDto) {}
