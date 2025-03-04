import { PartialType } from '@nestjs/mapped-types';

import { CreateEmailRoleDto } from '@/email-roles/dto/create-email-role.dto';

export class UpdateEmailRoleDto extends PartialType(CreateEmailRoleDto) {}
