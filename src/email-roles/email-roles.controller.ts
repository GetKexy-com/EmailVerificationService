import { Body, Controller, Get, Param, Post } from '@nestjs/common';
import { SkipThrottle } from '@nestjs/throttler';

import { CreateEmailRoleDto } from '@/email-roles/dto/create-email-role.dto';
import { EmailRolesService } from '@/email-roles/email-roles.service';

@Controller('email-roles')
export class EmailRolesController {
  constructor(private readonly emailRolesService: EmailRolesService) {}

  @SkipThrottle()
  @Post('')
  async createMany(@Body() createEmailRoleDtos: CreateEmailRoleDto[]) {
    return await this.emailRolesService.createMany(createEmailRoleDtos);
  }

  @Get(':role')
  findOne(@Param('role') role: string) {
    return this.emailRolesService.findOne(role);
  }
}
