import {
  BaseEntity,
  Column,
  CreateDateColumn,
  Entity,
  PrimaryGeneratedColumn,
} from 'typeorm';

export enum BulkFileStatus {
  QUEUED = 'queued',
  PENDING = 'pending',
  PROCESSING = 'processing',
  READY_FOR_GREY_LIST = 'ready_for_grey_list',
  GREY_LIST_CHECK_PROGRESS = 'grey_list_check_progress',
  GREY_LIST_CHECK_DONE = 'grey_list_check_done',
  COMPLETE = 'complete',
  DELETED = 'deleted',
}

@Entity('bulk_files') // sql table name === 'bulk_files'
export class BulkFile extends BaseEntity {
  @PrimaryGeneratedColumn()
  id: number;

  @Column({ type: 'varchar', length: 255 })
  file_path: string;

  @Column({ type: 'varchar', length: 255, nullable: true })
  file_original_name: string;

  @Column({ type: 'int', nullable: true })
  user_id: number;

  @Column({ type: 'varchar', length: 255, nullable: true })
  validation_file_path: string;

  @Column({
    type: 'enum',
    enum: BulkFileStatus,
    default: BulkFileStatus.PENDING,
  })
  file_status: BulkFileStatus;

  @Column({ type: 'int' })
  total_email_count: number;

  @Column({ type: 'int', nullable: true })
  valid_email_count: number;

  @Column({ type: 'int', nullable: true })
  invalid_email_count: number;

  @Column({ type: 'int', nullable: true })
  spam_trap_count: number;

  @Column({ type: 'int', nullable: true })
  unknown_count: number;

  @Column({ type: 'int', nullable: true })
  catch_all_count: number;

  @Column({ type: 'int', nullable: true })
  do_not_mail_count: number;

  @CreateDateColumn({ type: 'timestamptz' })
  created_at: Date;

  @CreateDateColumn({ type: 'timestamptz', default: null })
  updated_at: Date;
}

// SQl Query to create Hash Index
// `CREATE INDEX CONCURRENTLY email_hash_index on domains USING HASH (email_address)`
