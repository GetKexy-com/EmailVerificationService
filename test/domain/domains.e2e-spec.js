"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const testing_1 = require("@nestjs/testing");
const common_1 = require("@nestjs/common");
const domains_module_1 = require("../../src/domains/domains.module");
const typeorm_1 = require("@nestjs/typeorm");
const response_success_interceptor_1 = require("../../src/common/interceptors/response-success.interceptor");
const response_error_interceptor_1 = require("../../src/common/interceptors/response-error.interceptor");
const request = require("supertest");
const random_string_generator_util_1 = require("@nestjs/common/utils/random-string-generator.util");
const datasource_config_1 = require("../../src/database/config/datasource.config");
describe('AppController (e2e)', () => {
    const domain = {
        domain: (0, random_string_generator_util_1.randomStringGenerator)() + '.com',
        domain_ip: '234.45.0.24',
        domain_age_days: 334,
        mx_record_host: 'smtp.gmail.com'
    };
    let app;
    beforeAll(async () => {
        const moduleFixture = await testing_1.Test.createTestingModule({
            imports: [domains_module_1.DomainsModule, typeorm_1.TypeOrmModule.forRoot(datasource_config_1.datasourceOptions)],
        }).compile();
        app = moduleFixture.createNestApplication();
        app.useGlobalPipes(new common_1.ValidationPipe({
            whitelist: true,
            transform: true,
            forbidNonWhitelisted: true,
            transformOptions: {
                enableImplicitConversion: true,
            },
        }));
        app.useGlobalInterceptors(new response_success_interceptor_1.ResponseSuccessInterceptor(), new response_error_interceptor_1.ResponseErrorInterceptor());
        await app.init();
    });
    it.todo('Get all [GET /]');
    it.todo('Get one [GET /:domain]');
    it.todo('Update one [PATCH /:id]');
    it.todo('Delete one [DELETE /:id]');
    it('Create [POST /]', () => {
        return request(app.getHttpServer())
            .post('/domains')
            .send(domain)
            .expect(common_1.HttpStatus.CREATED);
    });
    afterAll(async () => {
        await app.close();
    });
});
//# sourceMappingURL=domains.e2e-spec.js.map