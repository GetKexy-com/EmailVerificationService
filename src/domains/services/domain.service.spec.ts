import { NotFoundException } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';

import AppDataSource from '@/database/config/datasource.config';
import { Domain } from '@/domains/entities/domain.entity';
import { DomainService } from '@/domains/services/domain.service';

let connection: any;
describe('DomainService', () => {
  let service: DomainService;

  beforeAll(async () => {
    AppDataSource.setOptions({
      synchronize: true,
      dropSchema: true,
    });
    connection = await AppDataSource.initialize();
    await connection.synchronize(true);
  });

  afterAll(async () => {
    await connection.close();
  });

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        DomainService,
        { provide: connection, useValue: {} },
        {
          provide: Domain,
          useValue: {},
        },
      ],
    }).compile();

    service = module.get<DomainService>(DomainService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  describe('findOne', () => {
    // describe('when domain exists', () => {
    //   it('should return the domain object', async () => {
    //     const domainId = 'yandex.com';
    //     const expectedDomain = {};
    //
    //     const domain = await service.findOne(domainId);
    //     expect(domain).toEqual(expectedDomain);
    //   });
    // });
    describe('when domain does not exists', () => {
      it('should throw the "NotFoundException"', async () => {
        const domainId = 'yandex.com';
        try {
          await service.findOne(domainId);
        } catch (e) {
          expect(e).toBeInstanceOf(NotFoundException);
          expect(e.message).toEqual(`Domain ${domainId} not found`);
        }
      });
    });
  });
});
