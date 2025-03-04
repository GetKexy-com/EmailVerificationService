import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';

import { EmailRolesController } from '@/email-roles/email-roles.controller';
import { EmailRolesService } from '@/email-roles/email-roles.service';
import { EmailRole } from '@/email-roles/entities/email-role.entity';

@Module({
  imports: [TypeOrmModule.forFeature([EmailRole])],
  controllers: [EmailRolesController],
  providers: [EmailRolesService],
  exports: [EmailRolesService],
})
export class EmailRolesModule {}
