import { Test, TestingModule } from '@nestjs/testing';

import { BulkFilesService } from '@/bulk-files/bulk-files.service';

describe('BulkFilesService', () => {
  let service: BulkFilesService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [BulkFilesService],
    }).compile();

    service = module.get<BulkFilesService>(BulkFilesService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
