import { Test, TestingModule } from '@nestjs/testing';

import { EmailRolesController } from './email-roles.controller';
import { EmailRolesService } from './email-roles.service';

describe('EmailRolesController', () => {
  let controller: EmailRolesController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [EmailRolesController],
      providers: [EmailRolesService],
    }).compile();

    controller = module.get<EmailRolesController>(EmailRolesController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
