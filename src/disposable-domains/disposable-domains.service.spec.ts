import { Test, TestingModule } from '@nestjs/testing';

import { DisposableDomainsService } from '@/disposable-domains/disposable-domains.service';

describe('DisposableDomainsService', () => {
  let service: DisposableDomainsService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [DisposableDomainsService],
    }).compile();

    service = module.get<DisposableDomainsService>(DisposableDomainsService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
