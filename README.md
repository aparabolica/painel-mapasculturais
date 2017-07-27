# Painel Mapas Culturais

> Ferramenta de análise do Mapas Culturais

## Pré-requisitos e instalação

Para rodar é necessário instalar o [software de virtualização Docker](https://www.docker.com). Ele está disponível para [Linux](https://www.docker.com/docker-ubuntu), [Mac](https://www.docker.com/docker-mac) ou [Windows](https://www.docker.com/docker-windows), no caso do Linux é necessário instalar também o [ docker-compose](https://docs.docker.com/compose/install). Também é necessário instalar o [Node.js](https://nodejs.org/en/download/).

Após instalar o Docker, [baixe este repositório](https://github.com/aparabolica/painel-mapasculturais/archive/master.zip) e descompacte em um diretório de trabalho. No terminal, execute o comando no diretório base do repositório:

```
docker-compose up
```

A primeira execução será mais demorada porque serão baixadas as imagens do [Metabase](metabase.com) e [MongoDB](www.mongodb.com). Será possíve acessar a ferramenta  quando for exibida a mensagem `Metabase Initialization COMPLETE` no terminal de execução. Então, acesse:

- http://localhost:3000
- login: admin@admin.org
- senha: admin1


# Obtendo dados das instalações

No diretório do repositório, instale as dependências com o `npm install` e rode `npm run update`.

# Referências

Mapas Culturais:
- [ API](https://github.com/hacklabr/mapasculturais/blob/develop/documentation/docs/mc_config_api.md)
- [código-fonte](https://github.com/hacklabr/mapasculturais)
