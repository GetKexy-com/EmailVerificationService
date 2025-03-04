"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const testing_1 = require("@nestjs/testing");
const request = require("supertest");
const app_module_1 = require("../src/app.module");
const process = require("node:process");
describe('AppController (e2e)', () => {
    let app;
    beforeAll(async () => {
        const moduleFixture = await testing_1.Test.createTestingModule({
            imports: [app_module_1.AppModule],
        }).compile();
        app = moduleFixture.createNestApplication();
        await app.init();
    });
    it('/ (GET)', () => {
        return request(app.getHttpServer())
            .get('/')
            .set('Authorization', process.env.API_KEY)
            .expect(200)
            .expect('Hello World!');
    });
    afterAll(async () => {
        await app.close();
    });
});
//# sourceMappingURL=app.e2e-spec.js.map