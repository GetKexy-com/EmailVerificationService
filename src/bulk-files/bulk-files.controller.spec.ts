import { Test, TestingModule } from '@nestjs/testing';

import { BulkFilesController } from '@/bulk-files/bulk-files.controller';
import { BulkFilesService } from '@/bulk-files/bulk-files.service';

describe('BulkFilesController', () => {
  let controller: BulkFilesController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [BulkFilesController],
      providers: [BulkFilesService],
    }).compile();

    controller = module.get<BulkFilesController>(BulkFilesController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
