import {
  BaseEntity,
  Column,
  CreateDateColumn,
  Entity,
  Index,
  PrimaryGeneratedColumn,
} from 'typeorm';

export enum UserStatus {
  ACTIVE = 'active',
  INACTIVE = 'inactive',
  PAUSED = 'paused',
  DELETED = 'deleted',
}

@Entity('users') // sql table name === 'users'
export class User extends BaseEntity {
  @PrimaryGeneratedColumn()
  id: number;

  @Column({ type: 'varchar', length: 64 })
  first_name: string;

  @Column({ type: 'varchar', length: 64 })
  last_name: string;

  @Column({ type: 'varchar', length: 128, nullable: true })
  timezone: string;

  @Column({ type: 'varchar', length: 255 })
  password: string;

  @Column({ type: 'varchar', length: 255 })
  @Index({ unique: true })
  email_address: string;

  @Column({ type: 'enum', enum: UserStatus, default: UserStatus.ACTIVE })
  status: UserStatus;

  @Column({ type: 'boolean', default: null })
  verify_plus: boolean;

  @CreateDateColumn({ type: 'timestamptz' })
  created_at: Date;
}

// SQl Query to create Hash Index
// `CREATE INDEX CONCURRENTLY user_email_hash_index on domains USING HASH (email_address)`
