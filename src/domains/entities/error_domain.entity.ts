import {
  BaseEntity,
  Column,
  CreateDateColumn,
  Entity,
  Index,
  PrimaryGeneratedColumn,
} from 'typeorm';

@Entity('error_domains') // sql table name === 'error_domains'
export class ErrorDomain extends BaseEntity {
  @PrimaryGeneratedColumn()
  id: number;

  @Column({ type: 'varchar', length: 255 })
  @Index()
  domain: string;

  @Column('json', { nullable: true })
  domain_error: {};

  @CreateDateColumn({ type: 'timestamptz' })
  readonly created_at: Date;
}

// SQl Query to create Hash Index
// `CREATE INDEX CONCURRENTLY domain_hash_index on domains USING HASH (domain)`
