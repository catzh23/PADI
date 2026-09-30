# Demonstrar o defeito da phase 1

O Step 1 pede implementar modos de debug que permitam demonstrar que o baseline
é incorreto. Este demo compara o processador antigo (`bogus`) com o corrigido
(`fixed`), usando o mesmo cenário e o código atual do servidor.

Na raiz do projeto, em Ubuntu/WSL com Java 22, Maven e Python 3:

```bash
mvn clean install
bash demo/bogus_phase1.sh bogus
bash demo/bogus_phase1.sh fixed
```

Cada execução inicia três servidores nas portas 9200–9202, dois clientes e uma
consola. Se as portas estiverem ocupadas, termina sem as reutilizar; podes escolher
outras com `PORT=9300 bash demo/bogus_phase1.sh bogus`.
O script termina apenas os processos que iniciou. Os logs de cada execução ficam
numa pasta diferente dentro de `demo/run/` e não são apagados pela execução seguinte.

## Cenário

1. S0 decide o pedido 101 (`sell 0 50`) no slot 0. O demo espera que as três réplicas
   o executem.
2. S1 recebe `debug crash 1` e é reiniciado. O seu histórico em memória perdeu-se.
3. S0 e S2 recebem `freeze`. Continuam a aceitar comandos da consola, mas as
   respostas ao prepare ficam bloqueadas. Só S1 pode responder imediatamente.
4. A consola inicia o ballot 1 em S1. O demo confirma nos logs que o comando chegou
   e que S1 iniciou a preparação, antes de avançar.
5. O cliente 2 envia o pedido 102 (`sell 3 50`).
6. S0 e S2 recebem `un-freeze`, libertando as mensagens pendentes.

Usar freeze em vez de atrasos aleatórios torna explícito o momento em que as
respostas ficam disponíveis. A consola é reiniciada depois do crash para não
reutilizar a ligação que estava em reconexão com o antigo S1.

## O que se verifica

Em **bogus**, a preparação termina com a resposta do próprio S1, que não tem
histórico. S1 propõe e executa 102 no slot 0; S0 e S2 já tinham executado 101 nesse
slot. O demo termina com `PASS: baseline defect reproduced` e exit code 0 quando
observa esta divergência. O sucesso do demo significa que reproduziu o defeito,
não que o algoritmo bogus esteja correto.

Em **fixed**, a preparação não pode terminar enquanto apenas S1 responde. Após
un-freeze, a maioria inclui S0 ou S2, permitindo recuperar 101. O demo verifica
que S1 propõe 101 e que o slot 0 é novamente decidido com esse valor. Termina com
`PASS: fixed phase 1 recovered 101` e exit code 0.

A segunda execução verifica a preservação da decisão, não recuperação completa
após um reinício: S1 conhece o ID 101, mas perdeu o corpo do pedido e não o consegue
executar sem o recuperar. Essa limitação é separada da seleção do valor na phase 1.
O cenário demonstra o defeito de quórum com um acceptor reiniciado sem histórico;
não constitui uma implementação de Paxos com recuperação persistente após crash.

Um timeout ou a ausência das condições esperadas produz exit code diferente de 0.
O erro identifica o processo e o log a consultar. Não se considera um timeout uma
prova de divergência.

## Ficheiros

- `bogus_phase1.sh`: sequência do cenário e condições verificadas.
- `lab.sh`: arranque, comunicação e limpeza dos processos próprios do demo.
- `PhaseOneBogusProcessor.java`: implementação intencionalmente incorreta,
  selecionada apenas com `-Ddidatrade.phase1=bogus`.

O modo normal continua a usar `PhaseOneResponseProcessor`.
