/*
** FFI C call handling.
** Copyright (C) 2005-2011 Mike Pall. See Copyright Notice in luajit.h
*/

#ifndef _LJ_CCALL_H
#define _LJ_CCALL_H

#include "lj_obj.h"

#if LJ_HASFFI

/* -- C calling conventions ----------------------------------------------- */

#if LJ_TARGET_X86ORX64

#if LJ_TARGET_X86
#define CCALL_NARG_GPR		2	/* For fastcall arguments. */
#define CCALL_NARG_FPR		0
#define CCALL_NRET_GPR		2
#define CCALL_NRET_FPR		1	/* For FP results on x87 stack. */
#define CCALL_ALIGN_STACKARG	0	/* Don't align argument on stack. */
#elif LJ_ABI_WIN
#define CCALL_NARG_GPR		4
#define CCALL_NARG_FPR		4
#define CCALL_NRET_GPR		1
#define CCALL_NRET_FPR		1
#define CCALL_SPS_EXTRA		4
#else
#define CCALL_NARG_GPR		6
#define CCALL_NARG_FPR		8
#define CCALL_NRET_GPR		2
#define CCALL_NRET_FPR		2
#define CCALL_VECTOR_REG	1	/* Pass vectors in registers. */
#endif

#define CCALL_SPS_FREE		1

typedef LJ_ALIGN(16) union FPRArg {
  double d[2];
  float f[4];
  uint8_t b[16];
  uint16_t s[8];
  int i[4];
  int64_t l[2];
} FPRArg;

typedef intptr_t GPRArg;

#elif LJ_TARGET_ARM

#define CCALL_NARG_GPR		4
#define CCALL_NARG_FPR		0
#define CCALL_NRET_GPR		2	/* For softfp double. */
#define CCALL_NRET_FPR		0
#define CCALL_SPS_FREE		0	/* NYI */

typedef intptr_t GPRArg;

#elif LJ_TARGET_PPCSPE

#define CCALL_NARG_GPR		8
#define CCALL_NARG_FPR		0
#define CCALL_NRET_GPR		4	/* For softfp complex double. */
#define CCALL_NRET_FPR		0
#define CCALL_SPS_FREE		0	/* NYI */

typedef intptr_t GPRArg;

#else
#error "missing calling convention definitions for this architecture"
#endif

#ifndef CCALL_SPS_EXTRA
#define CCALL_SPS_EXTRA		0
#endif
#ifndef CCALL_VECTOR_REG
#define CCALL_VECTOR_REG	0
#endif
#ifndef CCALL_ALIGN_STACKARG
#define CCALL_ALIGN_STACKARG	1
#endif

#define CCALL_NUM_GPR \
  (CCALL_NARG_GPR > CCALL_NRET_GPR ? CCALL_NARG_GPR : CCALL_NRET_GPR)
#define CCALL_NUM_FPR \
  (CCALL_NARG_FPR > CCALL_NRET_FPR ? CCALL_NARG_FPR : CCALL_NRET_FPR)

#define CCALL_MAXSTACK		32

/* -- C call state -------------------------------------------------------- */

typedef struct CCallState {
  void (*func)(void);		/* Pointer to called function. */
  uint32_t spadj;		/* Stack pointer adjustment. */
  uint8_t nsp;			/* Number of stack slots. */
  uint8_t retref;		/* Return value by reference. */
#if LJ_TARGET_X64
  uint8_t ngpr;			/* Number of arguments in GPRs. */
  uint8_t nfpr;			/* Number of arguments in FPRs. */
#elif LJ_TARGET_X86
  uint8_t resx87;		/* Result on x87 stack: 1:float, 2:double. */
#endif
#if CCALL_NUM_FPR
  FPRArg fpr[CCALL_NUM_FPR];	/* Arguments/results in FPRs. */
#endif
  GPRArg gpr[CCALL_NUM_GPR];	/* Arguments/results in GPRs. */
  GPRArg stack[CCALL_MAXSTACK];	/* Stack slots. */
} CCallState;

/* -- C call handling ----------------------------------------------------- */

/* Really belongs to lj_vm.h. */
LJ_ASMF void LJ_FASTCALL lj_vm_ffi_call(CCallState *cc);
LJ_FUNC int lj_ccall_func(lua_State *L, GCcdata *cd);

#endif

#endif
