import { Test, TestingModule } from '@nestjs/testing';
import { BulkFileEmailsService } from './bulk-file-emails.service';

describe('BulkFileEmailsService', () => {
  let service: BulkFileEmailsService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [BulkFileEmailsService],
    }).compile();

    service = module.get<BulkFileEmailsService>(BulkFileEmailsService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
