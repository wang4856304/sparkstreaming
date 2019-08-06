package com.wj.entity;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import javax.persistence.*;
import java.io.Serializable;

/**
 * @author jun.wang
 * @title: Warnning
 * @projectName ownerpro
 * @description: TODO
 * @date 2019/7/26 14:31
 */

@Data
@NoArgsConstructor
@Entity
@Table(name = "event")
public class Warnning implements Serializable {

    @Id
    @GeneratedValue
    private Long id;

    @Column(name = "tid")
    private String tId;

    @Column(name = "mid")
    private String mId;

    @Column(name = "did")
    private String dId;

    @Column(name = "times")
    private int times;

    @Column(name = "create_at")
    private String createAt;

    @Column(name = "update_at")
    private String updateAt;
}
