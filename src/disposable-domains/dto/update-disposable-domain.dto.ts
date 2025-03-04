import { PartialType } from '@nestjs/mapped-types';

import { CreateDisposableDomainDto } from './create-disposable-domain.dto';

export class UpdateDisposableDomainDto extends PartialType(
  CreateDisposableDomainDto,
) {}
