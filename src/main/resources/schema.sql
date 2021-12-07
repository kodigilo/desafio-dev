create table if not exists transacoes
(
    id        varchar(255) not null primary key,
    tipo      int unsigned   default null,
    data      date           default null,
    valor     decimal(22, 2) default null,
    cpf       varchar(255)   default null,
    cartao    varchar(255)   default null,
    hora      time           default null,
    dono_loja varchar(255)   default null,
    loja      varchar(255)   default null
);

