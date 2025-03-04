import {
  BaseEntity,
  Column,
  CreateDateColumn,
  Entity,
  PrimaryGeneratedColumn,
} from 'typeorm';

@Entity('disposable_domains') // sql table name === 'disposable_domains'
export class DisposableDomain extends BaseEntity {
  @PrimaryGeneratedColumn()
  id: number;

  @Column({ type: 'varchar', length: 255 })
  domain: string;

  @CreateDateColumn({ type: 'timestamptz' })
  readonly created_at: Date;
}

// SQl Query to create Hash Index
// `CREATE INDEX CONCURRENTLY disposable_domains_hash_index on disposable_domains USING HASH (domain)`
