import {
  BaseEntity,
  Column,
  CreateDateColumn,
  Entity,
  Index,
  PrimaryGeneratedColumn,
} from 'typeorm';

export type MXRecord = {
  exchange: string,
  priority: number
}

@Entity('domains') // sql table name === 'domains'
export class Domain extends BaseEntity {
  @PrimaryGeneratedColumn()
  id: number;

  @Column({ type: 'varchar', length: 255 })
  @Index()
  domain: string;

  @Column({ nullable: true, type: 'varchar', length: 64 })
  domain_ip: string;

  @Column({ nullable: true })
  domain_age_days: number;

  @Column()
  mx_record_hosts: string;

  @CreateDateColumn({ type: 'timestamptz' })
  readonly created_at: Date;
}

// SQl Query to create Hash Index
// `CREATE INDEX CONCURRENTLY domain_hash_index on domains USING HASH (domain)`
