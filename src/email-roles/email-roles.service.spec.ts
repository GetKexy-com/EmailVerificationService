import { Test, TestingModule } from '@nestjs/testing';

import { EmailRolesService } from './email-roles.service';

describe('EmailRolesService', () => {
  let service: EmailRolesService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [EmailRolesService],
    }).compile();

    service = module.get<EmailRolesService>(EmailRolesService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
