import React, { HTMLAttributes } from 'react';
import { css, cx } from '@emotion/css';
import { GrafanaTheme2 } from '@grafana/data';
import { styleMixins, stylesFactory, useStyles2, useTheme2 } from '../../themes';

/**
 * @public
 */
export interface CardInnerProps {
  href?: string;
  children?: React.ReactNode;
}

/** @deprecated This component will be removed in a future release */
const CardInner = ({ children, href }: CardInnerProps) => {
  const { inner } = useStyles2(getCardInnerStyles);
  return href ? (
    <a className={inner} href={href}>
      {children}
    </a>
  ) : (
    <>{children}</>
  );
};

const getCardInnerStyles = (theme: GrafanaTheme2) => ({
  inner: css({
    display: 'flex',
    width: '100%',
    padding: theme.spacing(2),
  }),
});

/**
 * @public
 */
export interface CardContainerProps extends HTMLAttributes<HTMLOrSVGElement>, CardInnerProps {
  /** Disable pointer events for the Card, e.g. click events */
  disableEvents?: boolean;
  /** No style change on hover */
  disableHover?: boolean;
  /** Custom container styles */
  className?: string;
}

/** @deprecated Using `CardContainer` directly is discouraged and should be replaced with `Card` */
export const CardContainer = ({
  children,
  disableEvents,
  disableHover,
  className,
  href,
  ...props
}: CardContainerProps) => {
  const theme = useTheme2();
  const { oldContainer } = getCardContainerStyles(theme, disableEvents, disableHover);
  return (
    <div {...props} className={cx(oldContainer, className)}>
      <CardInner href={href}>{children}</CardInner>
    </div>
  );
};

export const getCardContainerStyles = stylesFactory((theme: GrafanaTheme2, disabled = false, disableHover = false) => {
  return {
    container: css({
      display: 'grid',
      position: 'relative',
      gridTemplateColumns: 'auto 1fr auto',
      gridTemplateRows: '1fr auto auto auto',
      gridAutoColumns: '1fr',
      gridAutoFlow: 'row',
      gridTemplateAreas: `
        "Figure Heading Tags"
        "Figure Meta Tags"
        "Figure Description Tags"
        "Figure Actions Secondary"`,
      width: '100%',
      padding: theme.spacing(2),
      background: theme.colors.background.secondary,
      borderRadius: theme.shape.borderRadius(),
      pointerEvents: disabled ? 'none' : 'auto',
      transition: theme.transitions.create(['background-color', 'box-shadow', 'border-color', 'color'], {
        duration: theme.transitions.duration.short,
      }),

      ...(!disableHover && {
        '&:hover': {
          background: theme.colors.emphasize(theme.colors.background.secondary, 0.03),
          cursor: 'pointer',
          zIndex: 1,
        },
        '&:focus': styleMixins.getFocusStyles(theme),
      }),
    }),
    oldContainer: css({
      display: 'flex',
      width: '100%',
      background: theme.colors.background.secondary,
      borderRadius: theme.shape.borderRadius(),
      position: 'relative',
      pointerEvents: disabled ? 'none' : 'auto',
      transition: theme.transitions.create(['background-color', 'box-shadow', 'border-color', 'color'], {
        duration: theme.transitions.duration.short,
      }),

      ...(!disableHover && {
        '&:hover': {
          background: theme.colors.emphasize(theme.colors.background.secondary, 0.03),
          cursor: 'pointer',
          zIndex: 1,
        },
        '&:focus': styleMixins.getFocusStyles(theme),
      }),
    }),
  };
});
