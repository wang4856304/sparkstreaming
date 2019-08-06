package com.wj.repository;

import com.wj.entity.Warnning;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @author jun.wang
 * @title: WarningRepository
 * @projectName ownerpro
 * @description: TODO
 * @date 2019/8/5 17:14
 */
public interface WarningRepository extends JpaRepository<Warnning, Long> {
}
