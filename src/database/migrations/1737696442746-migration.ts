import { MigrationInterface, QueryRunner } from 'typeorm';

export class Migration1737696442746 implements MigrationInterface {
  name = 'Migration1737696442746';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP INDEX "public"."domain_hash_index"`);
    await queryRunner.query(`ALTER TABLE "domains" ADD "id" SERIAL NOT NULL`);
    await queryRunner.query(
      `ALTER TABLE "domains" DROP CONSTRAINT "PK_5346af016e911f3008ce7aa9a22"`,
    );
    await queryRunner.query(
      `ALTER TABLE "domains" ADD CONSTRAINT "PK_55d55f37b833b3bff8c9b992124" PRIMARY KEY ("domain", "id")`,
    );
    await queryRunner.query(
      `ALTER TABLE "domains" DROP CONSTRAINT "PK_55d55f37b833b3bff8c9b992124"`,
    );
    await queryRunner.query(
      `ALTER TABLE "domains" ADD CONSTRAINT "PK_05a6b087662191c2ea7f7ddfc4d" PRIMARY KEY ("id")`,
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE "domains" DROP CONSTRAINT "PK_05a6b087662191c2ea7f7ddfc4d"`,
    );
    await queryRunner.query(
      `ALTER TABLE "domains" ADD CONSTRAINT "PK_55d55f37b833b3bff8c9b992124" PRIMARY KEY ("domain", "id")`,
    );
    await queryRunner.query(
      `ALTER TABLE "domains" DROP CONSTRAINT "PK_55d55f37b833b3bff8c9b992124"`,
    );
    await queryRunner.query(
      `ALTER TABLE "domains" ADD CONSTRAINT "PK_5346af016e911f3008ce7aa9a22" PRIMARY KEY ("domain")`,
    );
    await queryRunner.query(`ALTER TABLE "domains" DROP COLUMN "id"`);
    await queryRunner.query(
      `CREATE INDEX "domain_hash_index" ON "domains" ("domain") `,
    );
  }
}
