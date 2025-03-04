import { MigrationInterface, QueryRunner } from 'typeorm';

export class Migration1737696911132 implements MigrationInterface {
  name = 'Migration1737696911132';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP INDEX "public"."domain_hash_index"`);
    await queryRunner.query(
      `CREATE TABLE "disposable_domains" ("id" SERIAL NOT NULL, "domain" character varying(255) NOT NULL, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_94b27fa8775f5a166004aa6d4b1" PRIMARY KEY ("id"))`,
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP TABLE "disposable_domains"`);
    await queryRunner.query(
      `CREATE INDEX "domain_hash_index" ON "domains" ("domain") `,
    );
  }
}
