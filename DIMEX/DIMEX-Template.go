/*  Construido como parte da disciplina: FPPD - PUCRS - Escola Politecnica
    Professor: Fernando Dotti  (https://fldotti.github.io/)
    Modulo representando Algoritmo de Exclusão Mútua Distribuída:
    Semestre 2023/1
	Aspectos a observar:
	   mapeamento de módulo para estrutura
	   inicializacao
	   semantica de concorrência: cada evento é atômico
	   							  módulo trata 1 por vez
	Q U E S T A O
	   Além de obviamente entender a estrutura ...
	   Implementar o núcleo do algoritmo ja descrito, ou seja, o corpo das
	   funcoes reativas a cada entrada possível:
	   			handleUponReqEntry()  // recebe do nivel de cima (app)
				handleUponReqExit()   // recebe do nivel de cima (app)
				handleUponDeliverRespOk(msgOutro)   // recebe do nivel de baixo
				handleUponDeliverReqEntry(msgOutro) // recebe do nivel de baixo
*/

package DIMEX

import (
	PP2PLink "SD/PP2PLink"
	"fmt"
	"strconv"
	"strings"
)

// ------------------------------------------------------------------------------------
// ------- principais tipos
// ------------------------------------------------------------------------------------

type State int // enumeracao dos estados possiveis de um processo
const (
	noMX State = iota
	wantMX
	inMX
)

type MessageTypes int // enumeracao dos tipos de mensagem
const (
	ENTRY MessageTypes = iota
	RESP
	EXIT
)

type dmxResp struct { // mensagem do módulo DIMEX infrmando que pode acessar - pode ser somente um sinal (vazio)
	// mensagem para aplicacao indicando que pode prosseguir
}

type DIMEX_Module struct {
	Req       chan MessageTypes // canal para receber pedidos da aplicacao (ENTRY e EXIT)
	Ind       chan dmxResp      // canal para informar aplicacao que pode acessar
	addresses []string          // endereco de todos, na mesma ordem
	id        int               // identificador do processo - é o indice no array de enderecos acima
	st        State             // estado deste processo na exclusao mutua distribuida
	waiting   []bool            // processos aguardando tem flag true
	lcl       int               // relogio logico local
	reqTs     int               // timestamp local da ultima requisicao deste processo
	nbrResps  int
	dbg       bool

	Pp2plink *PP2PLink.PP2PLink // acesso aa comunicacao enviar por PP2PLinq.Req  e receber por PP2PLinq.Ind
}

// ------------------------------------------------------------------------------------
// ------- inicializacao
// ------------------------------------------------------------------------------------

func NewDIMEX(_addresses []string, _id int, _dbg bool) *DIMEX_Module {

	p2p := PP2PLink.NewPP2PLink(_addresses[_id], _dbg)

	dmx := &DIMEX_Module{
		Req: make(chan MessageTypes, 1),
		Ind: make(chan dmxResp, 1),

		addresses: _addresses,
		id:        _id,
		st:        noMX,
		waiting:   make([]bool, len(_addresses)),
		lcl:       0,
		reqTs:     0,
		dbg:       _dbg,

		Pp2plink: p2p}

	for i := 0; i < len(dmx.waiting); i++ {
		dmx.waiting[i] = false
	}
	dmx.Start()
	dmx.outDbg("Init DIMEX!")
	return dmx
}

// ------------------------------------------------------------------------------------
// ------- nucleo do funcionamento
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) Start() {

	go func() {
		for {
			select {
			case dmxR := <-module.Req: // vindo da  aplicação
				if dmxR == ENTRY {
					module.outDbg("app pede mx")
					module.handleUponReqEntry() // ENTRADA DO ALGORITMO

				} else if dmxR == EXIT {
					module.outDbg("app libera mx")
					module.handleUponReqExit() // ENTRADA DO ALGORITMO
				}

			case msgOutro := <-module.Pp2plink.Ind: // vindo de outro processo
				messageType, _, _, err := module.decodeMessage(msgOutro.Message)
				if err != nil {
					module.outDbg("mensagem invalida: " + err.Error())
					continue
				}

				switch messageType {
				case RESP:
					module.outDbg("         <<<---- responde! " + msgOutro.Message)
					module.handleUponDeliverRespOk(msgOutro) // ENTRADA DO ALGORITMO

				case ENTRY:
					module.outDbg("          <<<---- pede??  " + msgOutro.Message)
					module.handleUponDeliverReqEntry(msgOutro) // ENTRADA DO ALGORITMO

				default:
					module.outDbg("tipo de mensagem inesperado: " + strconv.Itoa(int(messageType)))
				}
			}
		}
	}()
}

// ------------------------------------------------------------------------------------
// ------- tratamento de pedidos vindos da aplicacao
// ------- UPON ENTRY
// ------- UPON EXIT
// ------------------------------------------------------------------------------------
func (module *DIMEX_Module) handleUponReqEntry() {
	if module.st == noMX {
		module.lcl++
		module.reqTs = module.lcl
		module.nbrResps = 0
		module.st = wantMX

		//Agora é só enviarmos as mensagens com P2P.
		for i := range module.waiting {
			if i != module.id {
				module.sendToLink(module.addresses[i], module.buildMessage(ENTRY), "")
			}

			module.waiting[i] = false
		}
	}
}

func (module *DIMEX_Module) handleUponReqExit() {
	// Se eu recebi um Exit, o que eu quero é avisar todo mundo que ja fiz o que eu tinha que fazer, então é sair avisando isso
	module.st = noMX
	module.nbrResps = 0

	for i := range module.waiting {
		if i != module.id && module.waiting[i] {
			module.sendToLink(module.addresses[i], module.buildMessage(RESP), "")
		}

		module.waiting[i] = false
	}
}

// ------------------------------------------------------------------------------------
// ------- tratamento de mensagens de outros processos
// ------- UPON respOK
// ------- UPON reqEntry
// ------------------------------------------------------------------------------------
func (module *DIMEX_Module) handleUponDeliverRespOk(msgOutro PP2PLink.PP2PLink_Ind_Message) {
	if module.st == wantMX {
		module.nbrResps++

		if module.nbrResps == (len(module.addresses) - 1) {
			module.st = inMX
			module.Ind <- dmxResp{}
		}
	}
}

func (module *DIMEX_Module) handleUponDeliverReqEntry(msgOutro PP2PLink.PP2PLink_Ind_Message) {
	_, rcvLcl, pId, err := module.decodeMessage(msgOutro.Message)

	if err == nil && pId >= 0 && pId < len(module.addresses) && pId != module.id {
		newReqIsBefore := before(pId, rcvLcl, module.id, module.reqTs)

		if module.st == noMX || (module.st == wantMX && newReqIsBefore) {
			module.sendToLink(module.addresses[pId], module.buildMessage(RESP), "")
		} else if module.st == inMX || (module.st == wantMX && !newReqIsBefore) {
			module.waiting[pId] = true
		}

		module.lcl = max(module.lcl, rcvLcl)
	}
}

// ------------------------------------------------------------------------------------
// ------- funcoes de ajuda
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) sendToLink(address string, content string, space string) {
	module.outDbg(space + " ---->>>>   to: " + address + "     msg: " + content)
	module.Pp2plink.Req <- PP2PLink.PP2PLink_Req_Message{
		To:      address,
		Message: content}
}

func before(oneId, oneTs, othId, othTs int) bool {
	if oneTs < othTs {
		return true
	} else if oneTs > othTs {
		return false
	} else {
		return oneId < othId
	}
}

func (module *DIMEX_Module) outDbg(s string) {
	if module.dbg {
		fmt.Println(". . . . . . . . . . . . [ DIMEX : " + s + " ]")
	}
}

func (module *DIMEX_Module) buildMessage(messageType MessageTypes) string {
	return "||" + strconv.Itoa(int(messageType)) + "|" + strconv.Itoa(module.lcl) + "|" + strconv.Itoa(module.id) + "||"
}

func (module *DIMEX_Module) decodeMessage(message string) (messageType MessageTypes, rcvLcl int, pID int, err error) {
	if !strings.HasPrefix(message, "||") || !strings.HasSuffix(message, "||") {
		return 0, 0, 0, fmt.Errorf("formato de mensagem invalido: %q", message)
	}

	payload := strings.TrimSuffix(strings.TrimPrefix(message, "||"), "||")
	fields := strings.Split(payload, "|")
	if len(fields) != 3 {
		return 0, 0, 0, fmt.Errorf("mensagem deve possuir tipo, lcl e id: %q", message)
	}

	messageTypeValue, err := strconv.Atoi(fields[0])
	if err != nil {
		return 0, 0, 0, fmt.Errorf("tipo de mensagem invalido: %w", err)
	}
	if messageTypeValue < int(ENTRY) || messageTypeValue > int(EXIT) {
		return 0, 0, 0, fmt.Errorf("tipo de mensagem desconhecido: %d", messageTypeValue)
	}
	messageType = MessageTypes(messageTypeValue)

	rcvLcl, err = strconv.Atoi(fields[1])
	if err != nil {
		return 0, 0, 0, fmt.Errorf("lcl invalido: %w", err)
	}

	pID, err = strconv.Atoi(fields[2])
	if err != nil {
		return 0, 0, 0, fmt.Errorf("id de processo invalido: %w", err)
	}

	return messageType, rcvLcl, pID, nil
}
