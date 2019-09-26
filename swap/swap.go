// Copyright 2018 The Swarm Authors
// This file is part of the Swarm library.
//
// The Swarm library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The Swarm library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the Swarm library. If not, see <http://www.gnu.org/licenses/>.

package swap

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/console"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethersphere/swarm/contracts/swap"
	contract "github.com/ethersphere/swarm/contracts/swap"
	"github.com/ethersphere/swarm/p2p/protocols"
	"github.com/ethersphere/swarm/state"
)

// ErrInvalidChequeSignature indicates the signature on the cheque was invalid
var ErrInvalidChequeSignature = errors.New("invalid cheque signature")

var auditLog log.Logger // logger for Swap related messages and audit trail

// swapLogLevel indicates filter level of log messages
const swapLogLevel = 3

// Swap represents the Swarm Accounting Protocol
// a peer to peer micropayment system
// A node maintains an individual balance with every peer
// Only messages which have a price will be accounted for
type Swap struct {
	api              API
	store            state.Store        // store is needed in order to keep balances and cheques across sessions
	peers            map[enode.ID]*Peer // map of all swap Peers
	peersLock        sync.RWMutex       // lock for peers map
	backend          contract.Backend   // the backend (blockchain) used
	owner            *Owner             // contract access
	params           *Params            // economic and operational parameters
	backendNetworkID uint64             // backend network ID used by SWAP to settle payments
	contract         swap.Contract      // reference to the smart contract
	honeyPriceOracle HoneyOracle        // oracle which resolves the price of honey (in Wei)
}

// Owner encapsulates information related to accessing the contract
type Owner struct {
	address    common.Address    // owner address
	privateKey *ecdsa.PrivateKey // private key
	publicKey  *ecdsa.PublicKey  // public key
}

// Params encapsulates param
type Params struct {
	LogPath             string // optional audit log path
	PaymentThreshold    int64  // honey amount at which a payment is triggered
	DisconnectThreshold int64  // honey amount at which a peer disconnects
}

// newLogger returns a new logger
func newLogger(logpath string) log.Logger {
	swapLogger := log.New("swaplog", "*")
	lh := log.Root().GetHandler()

	if logpath == "" {
		swapLogger.SetHandler(lh)
		return swapLogger
	}

	rfh, err := swapRotatingFileHandler(logpath)

	if err != nil {
		log.Warn("RotatingFileHandler was not initialized", "logdir", logpath, "err", err)
		//sets a fallback logger, it will use the swarm logger.
		swapLogger.SetHandler(lh)
		return swapLogger
	}

	//Filters messages with the correct logLevel for swap
	rfh = log.LvlFilterHandler(log.Lvl(swapLogLevel), rfh)

	//Dispatches the logs to the default swarm log and also the filtered swap file logger.
	swapLogger.SetHandler(log.MultiHandler(lh, rfh))

	return swapLogger
}

// swapRotatingFileHandler returns a RotatingFileHandler this will split the logs into multiple files.
// the files are split based on the limit parameter expressed in bytes
func swapRotatingFileHandler(logdir string) (log.Handler, error) {
	return log.RotatingFileHandler(
		logdir,
		262144,
		log.JSONFormatOrderedEx(false, true),
	)
}

// new - swap constructor without integrity check
func new(stateStore state.Store, owner *Owner, backend contract.Backend, params *Params, backendNetworkID uint64, contract contract.Contract) *Swap {
	return &Swap{
		store:            stateStore,
		peers:            make(map[enode.ID]*Peer),
		backend:          backend,
		owner:            owner,
		params:           params,
		backendNetworkID: backendNetworkID,
		contract:         contract,
		honeyPriceOracle: NewHoneyPriceOracle(),
	}
}

// New - swap constructor with integrity checks
func New(dbPath string, prvkey *ecdsa.PrivateKey, backendURL string, params *Params, chequebookAddressFlag common.Address, initialDepositAmountFlag uint64) (*Swap, error) {
	// auditLog for swap-logging purposes
	auditLog = newLogger(params.LogPath)
	// verify that backendURL is not empty
	if backendURL == "" {
		return nil, errors.New("no backend URL given")
	}
	auditLog.Info("connecting to SWAP API", "url", backendURL)
	// initialize the balances store
	stateStore, err := state.NewDBStore(filepath.Join(dbPath, "swap.db"))
	if err != nil {
		return nil, fmt.Errorf("error while initializing statestore: %v", err)
	}
	// create the owner of SWAP
	owner := createOwner(prvkey)
	if params.DisconnectThreshold <= params.PaymentThreshold {
		return nil, fmt.Errorf("disconnect threshold lower or at payment threshold. DisconnectThreshold: %d, PaymentThreshold: %d", params.DisconnectThreshold, params.PaymentThreshold)
	}
	// connect to the backend
	backend, err := ethclient.Dial(backendURL)
	if err != nil {
		return nil, fmt.Errorf("error connecting to Ethereum API %s: %v", backendURL, err)
	}
	// get the networkID of the backend
	networkID, err := backend.NetworkID(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("error retrieving networkID from backendURL: %v", err)
	}
	// verify that we have not used SWAP before on a different networkID
	usedBefore, err := checkNetworkID(networkID.Uint64(), stateStore)
	if err != nil {
		return nil, fmt.Errorf("error checking network ID: %v", err)
	}
	// put the networkID in the statestore if this is the first time we initialize SWAP
	if !usedBefore {
		err = stateStore.Put(usedBeforeAtNetworkKey, networkID.Uint64())
		if err != nil {
			return nil, fmt.Errorf("error writing current backend networkID to statestore: %v", err)
		}
		auditLog.Info("SWAP initialized on backend network ID", "ID", networkID.Uint64())
	}
	// start the chequebook
	contract, err := StartChequebook(chequebookAddressFlag, initialDepositAmountFlag, stateStore, owner, backend)
	if err != nil {
		return nil, err
	}
	// create the SWAP instance
	return new(
		stateStore,
		owner,
		backend,
		params,
		networkID.Uint64(),
		contract,
	), nil
}

const (
	balancePrefix          = "balance_"
	sentChequePrefix       = "sent_cheque_"
	receivedChequePrefix   = "received_cheque_"
	usedChequebookKey      = "used_chequebook"
	usedBeforeAtNetworkKey = "used_before_at_network"
)

// checkNetworkID verifies whether we have initialized SWAP before and ensures that we are on the same backendNetworkID if this is the case
func checkNetworkID(currentNetworkID uint64, s state.Store) (usedBefore bool, err error) {
	var usedBeforeAtNetwork uint64
	err = s.Get(usedBeforeAtNetworkKey, &usedBeforeAtNetwork)
	// error reading from database
	if err != nil && err != state.ErrNotFound {
		return false, fmt.Errorf("error querying usedBeforeAtNetwork from statestore: %v", err)
	}
	if err == nil {
		usedBefore = true
	}
	// if used before on a different network, abort
	if usedBefore && usedBeforeAtNetwork != currentNetworkID {
		return false, fmt.Errorf("statestore previously used on different backend network. Used before on network: %d, Attempting to connect on network %d", usedBeforeAtNetwork, currentNetworkID)
	}
	return usedBefore, nil
}

// returns the store key for retrieving a peer's balance
func balanceKey(peer enode.ID) string {
	return balancePrefix + peer.String()
}

// returns the store key for retrieving a peer's last sent cheque
func sentChequeKey(peer enode.ID) string {
	return sentChequePrefix + peer.String()
}

// returns the store key for retrieving a peer's last received cheque
func receivedChequeKey(peer enode.ID) string {
	return receivedChequePrefix + peer.String()
}

func keyToID(key string, prefix string) enode.ID {
	return enode.HexID(key[len(prefix):])
}

// createOwner assings keys and addresses
func createOwner(prvkey *ecdsa.PrivateKey) *Owner {
	pubkey := &prvkey.PublicKey
	return &Owner{
		address:    crypto.PubkeyToAddress(*pubkey),
		privateKey: prvkey,
		publicKey:  pubkey,
	}
}

// Add is the (sole) accounting function
// Swap implements the protocols.Balance interface
func (s *Swap) Add(amount int64, peer *protocols.Peer) (err error) {
	swapPeer := s.getPeer(peer.ID())
	if swapPeer == nil {
		return fmt.Errorf("peer %s not a swap enabled peer", peer.ID().String())
	}
	swapPeer.lock.Lock()
	defer swapPeer.lock.Unlock()

	// Check if balance with peer is over the disconnect threshold
	balance := swapPeer.getBalance()
	if balance >= s.params.DisconnectThreshold {
		return fmt.Errorf("balance for peer %s is over the disconnect threshold %d, disconnecting", peer.ID().String(), s.params.DisconnectThreshold)
	}

	if err = swapPeer.updateBalance(amount); err != nil {
		return err
	}

	// Check if balance with peer crosses the payment threshold
	// It is the peer with a negative balance who sends a cheque, thus we check
	// that the balance is *below* the threshold
	if swapPeer.getBalance() <= -s.params.PaymentThreshold {
		auditLog.Info("balance for peer went over the payment threshold, sending cheque", "peer", peer.ID().String(), "payment threshold", s.params.PaymentThreshold)
		return swapPeer.sendCheque()
	}

	return nil
}

// handleMsg is for handling messages when receiving messages
func (s *Swap) handleMsg(p *Peer) func(ctx context.Context, msg interface{}) error {
	return func(ctx context.Context, msg interface{}) error {
		switch msg := msg.(type) {
		case *EmitChequeMsg:
			go s.handleEmitChequeMsg(ctx, p, msg)
		}
		return nil
	}
}

var defaultCashCheque = cashCheque

// handleEmitChequeMsg should be handled by the creditor when it receives
// a cheque from a debitor
func (s *Swap) handleEmitChequeMsg(ctx context.Context, p *Peer, msg *EmitChequeMsg) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	cheque := msg.Cheque
	auditLog.Info("received cheque from peer", "peer", p.ID().String(), "honey", cheque.Honey)
	_, err := s.processAndVerifyCheque(cheque, p)
	if err != nil {
		return err
	}

	auditLog.Debug("received cheque processed and verified", "peer", p.ID().String())

	// reset balance by amount
	// as this is done by the creditor, receiving the cheque, the amount should be negative,
	// so that updateBalance will calculate balance + amount which result in reducing the peer's balance
	if err := p.updateBalance(-int64(cheque.Honey)); err != nil {
		return err
	}

	opts := bind.NewKeyedTransactor(s.owner.privateKey)
	opts.Context = ctx

	otherSwap, err := contract.InstanceAt(cheque.Contract, s.backend)
	if err != nil {
		return err
	}

	// cash cheque in async, otherwise this blocks here until the TX is mined
	go defaultCashCheque(s, otherSwap, opts, cheque)

	return err
}

// cashCheque should be called async as it blocks until the transaction(s) are mined
// The function cashes the cheque by sending it to the blockchain
func cashCheque(s *Swap, otherSwap contract.Contract, opts *bind.TransactOpts, cheque *Cheque) {
	// blocks here, as we are waiting for the transaction to be mined
	result, receipt, err := otherSwap.CashChequeBeneficiary(opts, s.backend, s.GetParams().ContractAddress, big.NewInt(int64(cheque.CumulativePayout)), cheque.Signature)
	if err != nil {
		// TODO: do something with the error
		// and we actually need to log this error as we are in an async routine; nobody is handling this error for now
		auditLog.Error("error cashing cheque", "err", err)
		return
	}

	if result.Bounced {
		auditLog.Warn("cheque bounced", "tx", receipt.TxHash)
		return
		// TODO: do something here
	}

	auditLog.Debug("cash tx mined", "receipt", receipt)
}

// processAndVerifyCheque verifies the cheque and compares it with the last received cheque
// if the cheque is valid it will also be saved as the new last cheque
func (s *Swap) processAndVerifyCheque(cheque *Cheque, p *Peer) (uint64, error) {
	if err := cheque.verifyChequeProperties(p, s.owner.address); err != nil {
		return 0, err
	}

	lastCheque := p.getLastReceivedCheque()

	// TODO: there should probably be a lock here?
	expectedAmount, err := s.honeyPriceOracle.GetPrice(cheque.Honey)
	if err != nil {
		return 0, err
	}

	actualAmount, err := cheque.verifyChequeAgainstLast(lastCheque, expectedAmount)
	if err != nil {
		return 0, err
	}

	if err := p.setLastReceivedCheque(cheque); err != nil {
		auditLog.Error("error while saving last received cheque", "peer", p.ID().String(), "err", err.Error())
		// TODO: what do we do here? Related issue: https://github.com/ethersphere/swarm/issues/1515
	}

	return actualAmount, nil
}

// Balance returns the balance for a given peer
func (s *Swap) Balance(peer enode.ID) (int64, error) {
	swapPeer := s.getPeer(peer)
	if swapPeer == nil {
		return 0, state.ErrNotFound
	}
	return swapPeer.getBalance(), nil
}

// Balances returns the balances for all known SWAP peers
func (s *Swap) Balances() (map[enode.ID]int64, error) {
	balances := make(map[enode.ID]int64)

	s.peersLock.Lock()
	for peer, swapPeer := range s.peers {
		swapPeer.lock.Lock()
		balances[peer] = swapPeer.getBalance()
		swapPeer.lock.Unlock()
	}
	s.peersLock.Unlock()

	// add store balances, if peer was not already added
	balanceIterFunction := func(key []byte, value []byte) (stop bool, err error) {
		peer := keyToID(string(key), balancePrefix)
		if _, peerHasBalance := balances[peer]; !peerHasBalance {
			var peerBalance int64
			err = json.Unmarshal(value, &peerBalance)
			if err == nil {
				balances[peer] = peerBalance
			}
		}
		return stop, err
	}
	err := s.store.Iterate(balancePrefix, balanceIterFunction)
	if err != nil {
		return nil, err
	}

	return balances, nil
}

// loadLastReceivedCheque loads the last received cheque for the peer from the store
// and returns nil when there never was a cheque saved
func (s *Swap) loadLastReceivedCheque(p enode.ID) (cheque *Cheque, err error) {
	err = s.store.Get(receivedChequeKey(p), &cheque)
	if err == state.ErrNotFound {
		return nil, nil
	}
	return cheque, err
}

// loadLastSentCheque loads the last sent cheque for the peer from the store
// and returns nil when there never was a cheque saved
func (s *Swap) loadLastSentCheque(p enode.ID) (cheque *Cheque, err error) {
	err = s.store.Get(sentChequeKey(p), &cheque)
	if err == state.ErrNotFound {
		return nil, nil
	}
	return cheque, err
}

// loadBalance loads the current balance for the peer from the store
// and returns 0 if there was no prior balance saved
func (s *Swap) loadBalance(p enode.ID) (balance int64, err error) {
	err = s.store.Get(balanceKey(p), &balance)
	if err == state.ErrNotFound {
		return 0, nil
	}
	return balance, err
}

// saveLastReceivedCheque saves cheque as the last received cheque for peer
func (s *Swap) saveLastReceivedCheque(p enode.ID, cheque *Cheque) error {
	return s.store.Put(receivedChequeKey(p), cheque)
}

// saveLastSentCheque saves cheque as the last received cheque for peer
func (s *Swap) saveLastSentCheque(p enode.ID, cheque *Cheque) error {
	return s.store.Put(sentChequeKey(p), cheque)
}

// saveBalance saves balance as the current balance for peer
func (s *Swap) saveBalance(p enode.ID, balance int64) error {
	return s.store.Put(balanceKey(p), balance)
}

// Close cleans up swap
func (s *Swap) Close() error {
	return s.store.Close()
}

// GetParams returns contract parameters (Bin, ABI, contractAddress) from the contract
func (s *Swap) GetParams() *swap.Params {
	return s.contract.ContractParams()
}

// getContractOwner retrieve the owner of the chequebook at address from the blockchain
func (s *Swap) getContractOwner(ctx context.Context, address common.Address) (common.Address, error) {
	contr, err := contract.InstanceAt(address, s.backend)
	if err != nil {
		return common.Address{}, err
	}

	return contr.Issuer(nil)
}

func promptInitialDepositAmount() (uint64, error) {
	// need to prompt user for initial deposit amount
	// if 0, can not cash in cheques
	prompter := console.Stdin

	// ask user for input
	input, err := prompter.PromptInput("Please provide the amount in Wei which will deposited to your chequebook upon deployment: ")
	if err != nil {
		return 0, err
	}
	// check input
	val, err := strconv.ParseInt(input, 10, 64)
	if err != nil {
		// maybe we should provide a fallback here? A bad input results in stopping the boot
		return 0, fmt.Errorf("Conversion error while reading user input: %v", err)
	}
	return uint64(val), nil
}

// StartChequebook start the chequebook, taking into account the chequebookAddress passed in by the user and the chequebook addresses saved on the node's database
func StartChequebook(chequebookAddrFlag common.Address, initialDepositAmount uint64, s state.Store, owner *Owner, backend contract.Backend) (contract.Contract, error) {
	previouslyUsedChequebook, err := loadChequebook(s)
	// error reading from disk
	if err != nil && err != state.ErrNotFound {
		return nil, fmt.Errorf("Error reading previously used chequebook: %s", err)
	}
	// nothing written to state disk before, no flag provided: deploying new chequebook
	if err == state.ErrNotFound && chequebookAddrFlag == (common.Address{}) {
		var toDeposit = initialDepositAmount
		if toDeposit == 0 {
			toDeposit, err = promptInitialDepositAmount()
			if err != nil {
				return nil, err
			}
		}
		contract, err := Deploy(context.TODO(), toDeposit, owner, backend)
		if err != nil {
			return nil, err
		}
		err = saveChequebook(contract.ContractParams().ContractAddress, s)
		if err != nil {
			return nil, err
		}
		auditLog.Info("Binded to chequebook", "chequebookAddr", contract.ContractParams().ContractAddress)
		return contract, nil
	}

	// read from state, but provided flag is not the same
	if err == nil && (chequebookAddrFlag != common.Address{} && chequebookAddrFlag != previouslyUsedChequebook) {
		return nil, fmt.Errorf("Attempting to connect to provided chequebook, but different chequebook used before")
	}
	if chequebookAddrFlag != (common.Address{}) {
		return bindToContractAt(chequebookAddrFlag, backend)
	}
	return bindToContractAt(previouslyUsedChequebook, backend)
}

// BindToContractAt binds to an instance of an already existing chequebook contract at address
func bindToContractAt(address common.Address, backend contract.Backend) (contract.Contract, error) {
	// validate whether address is a chequebook
	if err := contract.ValidateCode(context.Background(), backend, address); err != nil {
		return nil, fmt.Errorf("contract validation for %v failed: %v", address.Hex(), err)
	}
	auditLog.Info("Binded to chequebook", "chequebookAddr", address)
	// get the instance
	return contract.InstanceAt(address, backend)
}

// Deploy deploys the Swap contract
func Deploy(ctx context.Context, initialDepositAmount uint64, owner *Owner, backend contract.Backend) (contract.Contract, error) {
	opts := bind.NewKeyedTransactor(owner.privateKey)
	// initial topup value
	opts.Value = big.NewInt(int64(initialDepositAmount))
	opts.Context = ctx

	auditLog.Info("deploying new swap", "owner", opts.From.Hex(), "deposit", opts.Value)
	return deployLoop(opts, defaultHarddepositTimeoutDuration, owner.address, backend)
}

// deployLoop repeatedly tries to deploy the swap contract .
func deployLoop(opts *bind.TransactOpts, defaultHarddepositTimeoutDuration time.Duration, owner common.Address, backend contract.Backend) (instance contract.Contract, err error) {
	var tx *types.Transaction
	for try := 0; try < deployRetries; try++ {
		if try > 0 {
			time.Sleep(deployDelay)
		}
		if instance, tx, err = contract.Deploy(opts, backend, owner, defaultHarddepositTimeoutDuration); err != nil {
			auditLog.Warn("can't send chequebook deploy tx", "try", try, "error", err)
			continue
		}
		if _, err := bind.WaitDeployed(opts.Context, backend, tx); err != nil {
			auditLog.Warn("chequebook deploy error", "try", try, "error", err)
			continue
		}
		return instance, nil
	}
	return nil, err
}

func loadChequebook(s state.Store) (common.Address, error) {
	var chequebook common.Address
	err := s.Get(usedChequebookKey, &chequebook)
	return chequebook, err
}

func saveChequebook(chequebook common.Address, s state.Store) error {
	return s.Put(usedChequebookKey, chequebook)
}
