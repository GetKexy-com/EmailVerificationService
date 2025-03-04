import { BaseEntity, Column, CreateDateColumn, Entity, Index, PrimaryGeneratedColumn } from 'typeorm';

@Entity('bulk_file_emails') // sql table name === 'domains'
export class BulkFileEmail extends BaseEntity {
  @PrimaryGeneratedColumn()
  id: number;

  @Column({ type: 'int', nullable: true })
  @Index()
  user_id: number;

  @Column({ type: 'int', nullable: true })
  @Index()
  bulk_file_id: number;

  @Column({ type: 'varchar', length: 255 })
  email_address: string;

  @CreateDateColumn({ type: 'timestamptz' })
  created_at?: Date;

}
