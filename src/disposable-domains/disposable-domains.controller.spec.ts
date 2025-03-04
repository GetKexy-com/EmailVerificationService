import { Test, TestingModule } from '@nestjs/testing';

import { DisposableDomainsController } from './disposable-domains.controller';
import { DisposableDomainsService } from './disposable-domains.service';

describe('DisposableDomainsController', () => {
  let controller: DisposableDomainsController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [DisposableDomainsController],
      providers: [DisposableDomainsService],
    }).compile();

    controller = module.get<DisposableDomainsController>(
      DisposableDomainsController,
    );
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
