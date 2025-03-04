import { Test, TestingModule } from '@nestjs/testing';
import { SmtpConnectionService } from './smtp-connection.service';

describe('SmtpConnectionService', () => {
  let service: SmtpConnectionService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [SmtpConnectionService],
    }).compile();

    service = module.get<SmtpConnectionService>(SmtpConnectionService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
