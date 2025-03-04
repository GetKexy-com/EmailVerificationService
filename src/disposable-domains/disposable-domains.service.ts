import { HttpException, HttpStatus, Injectable } from '@nestjs/common';
import { DataSource } from 'typeorm';

import { DisposableDomain } from '@/disposable-domains/entities/disposable-domain.entity';
import { CreateDisposableDomainDto } from '@/disposable-domains/dto/create-disposable-domain.dto';

@Injectable()
export class DisposableDomainsService {
  constructor(private dataSource: DataSource) {}

  create(createDisposableDomainDto: CreateDisposableDomainDto) {
    return 'This action adds a new disposableDomain';
  }

  async findByDomain(domain: string) {
    return await DisposableDomain.findOneBy({
      domain,
    });
  }

  async createMany(domains: DisposableDomain[]) {
    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();
    try {
      await queryRunner.manager.save(DisposableDomain, domains);
      await queryRunner.commitTransaction();
    } catch (err) {
      // since we have errors lets rollback the changes we made
      await queryRunner.rollbackTransaction();
      throw new HttpException(err.message, HttpStatus.BAD_REQUEST);
    } finally {
      // you need to release a queryRunner which was manually instantiated
      await queryRunner.release();
    }
  }
}
