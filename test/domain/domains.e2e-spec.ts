import { Test, TestingModule } from '@nestjs/testing';
import { HttpStatus, INestApplication, ValidationPipe } from '@nestjs/common';
import { DomainsModule } from '@/domains/domains.module';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ResponseSuccessInterceptor } from '@/common/interceptors/response-success.interceptor';
import { ResponseErrorInterceptor } from '@/common/interceptors/response-error.interceptor';
import * as request from 'supertest';
import { CreateDomainDto } from '@/domains/dto/create-domain.dto';
import { randomStringGenerator } from '@nestjs/common/utils/random-string-generator.util';
import { datasourceOptions } from '@/database/config/datasource.config';

describe('AppController (e2e)', () => {
  const domain = {
    domain: randomStringGenerator() + '.com',
    domain_ip: '234.45.0.24',
    domain_age_days: 334,
    mx_record_hosts: 'smtp.gmail.com'
  };

  let app: INestApplication;

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [DomainsModule, TypeOrmModule.forRoot(datasourceOptions)],
    }).compile();

    app = moduleFixture.createNestApplication();
    app.useGlobalPipes(
      new ValidationPipe({
        whitelist: true,
        transform: true,
        forbidNonWhitelisted: true,
        transformOptions: {
          enableImplicitConversion: true,
        },
      }),
    );
    app.useGlobalInterceptors(
      new ResponseSuccessInterceptor(),
      new ResponseErrorInterceptor(),
    );
    await app.init();
  });

  it.todo('Get all [GET /]');
  it.todo('Get one [GET /:domain]');
  it.todo('Update one [PATCH /:id]');
  it.todo('Delete one [DELETE /:id]');

  it('Create [POST /]', () => {
    return request(app.getHttpServer())
      .post('/domains')
      .send(domain as CreateDomainDto)
      .expect(HttpStatus.CREATED);
  });
  // it('Get all [GET /]', () => {});
  // it('Get one [GET /:domain]', () => {});
  // it('Update one [PATCH /:id]', () => {});
  // it('Delete one [DELETE /:id]', () => {});

  afterAll(async () => {
    await app.close();
  });
});
