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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
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
	TAKE_SNAPSHOT
)

type dmxResp struct { // mensagem do módulo DIMEX infrmando que pode acessar - pode ser somente um sinal (vazio)
	// mensagem para aplicacao indicando que pode prosseguir
}

// SnapshotMessage representa uma mensagem normal do DiMEx que estava em
// transito quando o snapshot foi realizado.
type SnapshotMessage struct {
	Type  MessageTypes
	Clock int
	From  int
}

// IncomingChannelSnapshot representa o canal logico From -> processo atual.
// Messages e a fila FIFO de mensagens registradas nesse canal.
type IncomingChannelSnapshot struct {
	From           int
	MarkerReceived bool
	Messages       []SnapshotMessage
}

// DIMEXStateSnapshot e uma copia das variaveis locais do DiMEx.
type DIMEXStateSnapshot struct {
	State     State
	Waiting   []bool
	Clock     int
	RequestTS int
	Responses int
}

// Snapshot e a contribuicao deste processo para um snapshot global.
type Snapshot struct {
	ID               int
	ProcessID        int
	DIMEXState       DIMEXStateSnapshot
	IncomingChannels []IncomingChannelSnapshot
}

type DIMEX_Module struct {
	Req             chan MessageTypes // canal para receber pedidos da aplicacao (ENTRY e EXIT)
	Ind             chan dmxResp      // canal para informar aplicacao que pode acessar
	addresses       []string          // endereco de todos, na mesma ordem
	id              int               // identificador deste processo
	st              State             // estado deste processo na exclusao mutua distribuida
	waiting         []bool            // processos aguardando tem flag true
	lcl             int               // relogio logico local
	reqTs           int               // timestamp local da ultima requisicao deste processo
	nbrResps        int
	dbg             bool
	canDoSnapshots  bool               // indica se este processo inicia snapshots
	snapshotRequest chan struct{}      // pedidos locais gerados pelo temporizador
	nextSnapshotID  int                // proximo ID criado pelo processo iniciador
	currentSnapshot *Snapshot          // nil quando nao ha snapshot local em andamento
	Pp2plink        *PP2PLink.PP2PLink // acesso aa comunicacao enviar por PP2PLinq.Req  e receber por PP2PLinq.Ind
}

// ------------------------------------------------------------------------------------
// ------- inicializacao
// ------------------------------------------------------------------------------------

func NewDIMEX(_addresses []string, _id int, _snapshot bool, _dbg bool) *DIMEX_Module {

	p2p := PP2PLink.NewPP2PLink(_addresses[_id], _dbg)

	dmx := &DIMEX_Module{
		Req: make(chan MessageTypes, 1),
		Ind: make(chan dmxResp, 1),

		addresses:       _addresses,
		id:              _id,
		st:              noMX,
		waiting:         make([]bool, len(_addresses)),
		lcl:             0,
		reqTs:           0,
		dbg:             _dbg,
		canDoSnapshots:  _snapshot,
		snapshotRequest: make(chan struct{}, 1),
		Pp2plink:        p2p}

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

	if module.canDoSnapshots {
		go func() {
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()

			for range ticker.C {
				module.snapshotRequest <- struct{}{}
			}
		}()
	}

	go func() {
		for {
			select {
			case <-module.snapshotRequest:
				module.handleLocalSnapshotRequest()

			case dmxR := <-module.Req: // vindo da  aplicação
				if dmxR == ENTRY {
					module.outDbg("app pede mx")
					module.handleUponReqEntry() // ENTRADA DO ALGORITMO

				} else if dmxR == EXIT {
					module.outDbg("app libera mx")
					module.handleUponReqExit() // ENTRADA DO ALGORITMO
				}
			case msgOutro := <-module.Pp2plink.Ind: // vindo de outro processo
				messageType, messageValue, senderID, err := module.decodeMessage(msgOutro.Message)
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

				case TAKE_SNAPSHOT:
					module.outDbg("<<<---- Take Snapshot  " + msgOutro.Message)
					module.TakeSnapshot(messageValue, senderID)

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

func (module *DIMEX_Module) SendTakeSnapshot(snapshotID int) {
	message := module.buildSnapshotMessage(snapshotID)

	for processID, address := range module.addresses {
		if processID == module.id {
			continue
		}

		module.sendToLink(address, message, "")
	}
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

func (module *DIMEX_Module) buildSnapshotMessage(snapshotID int) string {
	return "||" + strconv.Itoa(int(TAKE_SNAPSHOT)) + "|" + strconv.Itoa(snapshotID) + "|" + strconv.Itoa(module.id) + "||"
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
	if messageTypeValue < int(ENTRY) || messageTypeValue > int(TAKE_SNAPSHOT) {
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

func (module *DIMEX_Module) handleLocalSnapshotRequest() {
	if module.currentSnapshot != nil {
		module.outDbg(
			"nao inicia outro snapshot: existe um em andamento",
		)
	} else {
		module.nextSnapshotID++
		snapshotID := module.nextSnapshotID

		// Primeiro grava o estado local.
		module.startLocalSnapshot(snapshotID)

		// Depois envia o marcador.
		module.SendTakeSnapshot(snapshotID)
	}
}

func (module *DIMEX_Module) startLocalSnapshot(snapshotID int) {
	snap := Snapshot{
		ID:        snapshotID,
		ProcessID: module.id,

		DIMEXState: DIMEXStateSnapshot{
			State:     module.st,
			Waiting:   append([]bool(nil), module.waiting...),
			Clock:     module.lcl,
			RequestTS: module.reqTs,
			Responses: module.nbrResps,
		},

		IncomingChannels: make(
			[]IncomingChannelSnapshot,
			0,
			len(module.addresses)-1,
		),
	}

	// Existe um canal de entrada logico para cada outro processo.
	for processID := range module.addresses {
		if processID == module.id {
			continue
		}

		snap.IncomingChannels = append(
			snap.IncomingChannels,
			IncomingChannelSnapshot{
				From:           processID,
				MarkerReceived: false,
				Messages:       []SnapshotMessage{},
			},
		)
	}

	// O arquivo so sera salvo quando os marcadores chegarem por todos os canais.
	module.currentSnapshot = &snap
	module.outDbg("estado local gravado para o snapshot " + strconv.Itoa(snapshotID))
}

// TakeSnapshot trata o marcador recebido pela rede.
func (module *DIMEX_Module) TakeSnapshot(snapshotID, senderID int) {
	if snapshotID <= 0 || senderID < 0 || senderID >= len(module.addresses) || senderID == module.id {
		module.outDbg("marcador de snapshot invalido")
		return
	}

	if module.currentSnapshot == nil {
		// Primeiro marcador: grava o estado local.
		module.startLocalSnapshot(snapshotID)

		// O estado do canal que trouxe o primeiro marcador e vazio.
		module.markSnapshotChannel(senderID)

		// Propaga o mesmo marcador para todos os outros processos.
		module.SendTakeSnapshot(snapshotID)
	} else if module.currentSnapshot.ID == snapshotID {
		// Proximo marcador: encerra a gravacao do canal do remetente.
		module.markSnapshotChannel(senderID)
	} else {
		module.outDbg("marcador ignorado: outro snapshot esta em andamento")
		return
	}

	module.finishSnapshotIfComplete()
}

func (module *DIMEX_Module) markSnapshotChannel(senderID int) {
	if module.currentSnapshot == nil {
		return
	}

	for index := range module.currentSnapshot.IncomingChannels {
		channel := &module.currentSnapshot.IncomingChannels[index]
		if channel.From == senderID {
			channel.MarkerReceived = true
			return
		}
	}
}

func (module *DIMEX_Module) finishSnapshotIfComplete() {
	if module.currentSnapshot == nil {
		return
	}

	for _, channel := range module.currentSnapshot.IncomingChannels {
		if !channel.MarkerReceived {
			return
		}
	}

	snapshotID := module.currentSnapshot.ID
	if err := module.saveSnapshotToFile(module.currentSnapshot); err != nil {
		module.outDbg("erro ao salvar snapshot " + strconv.Itoa(snapshotID) + ": " + err.Error())
		return
	}

	module.currentSnapshot = nil
	module.outDbg("snapshot local " + strconv.Itoa(snapshotID) + " concluido")
}

func (module *DIMEX_Module) saveSnapshotToFile(
	snapshot *Snapshot,
) error {
	if err := os.MkdirAll("snapshots", 0755); err != nil {
		return err
	}

	filename := filepath.Join(
		"snapshots",
		fmt.Sprintf(
			"snapshot_%d_process_%d.txt",
			snapshot.ID,
			snapshot.ProcessID,
		),
	)

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = fmt.Fprintf(
		file,
		"snapshot_id: %d\n"+
			"process_id: %d\n"+
			"state: %s\n"+
			"logical_clock: %d\n"+
			"request_timestamp: %d\n"+
			"responses: %d\n"+
			"waiting: %v\n",
		snapshot.ID,
		snapshot.ProcessID,
		stateLabel(snapshot.DIMEXState.State),
		snapshot.DIMEXState.Clock,
		snapshot.DIMEXState.RequestTS,
		snapshot.DIMEXState.Responses,
		snapshot.DIMEXState.Waiting,
	)
	if err != nil {
		return err
	}

	for _, channel := range snapshot.IncomingChannels {
		_, err = fmt.Fprintf(
			file,
			"\nchannel_from: %d\n"+
				"marker_received: %t\n"+
				"messages: %d\n",
			channel.From,
			channel.MarkerReceived,
			len(channel.Messages),
		)
		if err != nil {
			return err
		}

		for _, message := range channel.Messages {
			_, err = fmt.Fprintf(
				file,
				"  message: type=%d clock=%d from=%d\n",
				message.Type,
				message.Clock,
				message.From,
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func stateLabel(state State) string {
	switch state {
	case noMX:
		return "noMX"
	case wantMX:
		return "wantMX"
	case inMX:
		return "inMX"
	default:
		return "unknown"
	}
}
